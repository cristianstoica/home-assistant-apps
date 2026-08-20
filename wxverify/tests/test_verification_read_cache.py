"""§8 oracles O1-O26 for ``wxverify.verification.read_cache`` (0.13.3).

Fixtures follow the house idiom in ``tests/test_verification_daily_rank_
conclusion.py``: a synthetic site (``site-alpha`` / ``site-beta`` / ...,
``40.0 / -105.0``, ``Etc/GMT+7``), hand-built constant-error evidence, a real
migrated database, no mocks of the datastore. Every mutant a claim below is
verified against is executed, not argued: the module's own scratch mutation
run (rebind the named symbol, run the oracle, record FAIL, restore, record
PASS) is captured in the release's QA report, not asserted here as prose.

Mutant table (M1-M18), executed against a scratch rebind of the production
symbol named, oracle FAILED then PASSED after restore:

| Mutant | Rebind | Oracle |
| --- | --- | --- |
| M1  key drops run_id                                    | O13 |
| M2  published gate before the insert only                | O17 |
| M3  LRU-only, no pinned tier                              | O10(a) |
| M4  _PINNED.update(...) instead of replace                | O16 |
| M5  insert before copy                                    | O11b(ii) |
| M6  stripe acquired without `with`, released only on success | O15 |
| M7  warm catches BaseException                            | O14 |
| M8  reconciliation collects keys from _ENTRIES             | O18 |
| M9  warm awaited before worker in the lifespan finally     | O20b |
| M10 an always-recompute wrapper                           | O5 |
| M11 derivation computed outside the stripe                 | O9 |
| M12 reconciliation with no epoch arbitration                | O22 |
| M13 insert with no re-check of the key's generation         | O23 |
| M14 lifespan finally suppresses the warm's exception         | O20 |
| M15 pointer snapshot resolved one site at a time             | O25 |
| M16 ticket taken outside the snapshot's critical section      | O24 |
| M17 reset_read_cache clears tiers without touching _warm_epoch | O26 |
| M18 reset_read_cache bumps _warm_epoch but not _reset_token     | O26 |

Results for each: FAIL recorded against the mutant, PASS recorded after
restore; this docstring records the claim, the mutation run is the evidence.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import re
import sqlite3
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar, cast

import pytest
from fastapi.testclient import TestClient

from tests.helpers import asof_conn
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification import read_cache as rc
from wxverify.verification.diagnostics import observed_wet_precip_mae
from wxverify.verification.ranking import DAILY_RANK_ENTITY_TYPE, daily_rank_conclusions
from wxverify.verification.runs import (
    capture_config_snapshot,
    publish_run,
    published_run_key,
)
from wxverify.verification.simulate import SIM_DEPTHS, SIM_VARIABLES

_ReadT = TypeVar("_ReadT")

_LEAD = 1
_WIND_TRUTH = 10.0
_WET_TRUTH = 5.0
_DRY_TRUTH = 0.0
_DEPTH = 2
_WIND_DATES = [f"2026-05-{d:02d}" for d in range(1, 29)][:24]

_fp_counter = 0


# ---------------------------------------------------------------------------
# Shared fixture builders
# ---------------------------------------------------------------------------


def _threaded_conn() -> sqlite3.Connection:
    """A fully-migrated in-memory database usable from more than one thread."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _site(
    conn: sqlite3.Connection, name: str = "site-alpha", *, enabled: bool = True
) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                           timezone, enabled)
        VALUES (?, 40.0, -105.0, 900.0, 'Etc/GMT+7', ?)
        """,
        (name, 1 if enabled else 0),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _new_run(conn: sqlite3.Connection, site_id: int, *, state: str = "running") -> int:
    global _fp_counter
    _fp_counter += 1
    generation_id = ensure_published_generation(conn, site_id)
    snapshot = capture_config_snapshot(conn, site_id)
    depths = {v: _DEPTH for v in SIM_VARIABLES}
    snapshot["blend_depth"] = _DEPTH
    snapshot["blend_depths"] = dict(depths)
    cur = conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version, state,
             attempt, config_snapshot, period_start, period_end, settled_through,
             bootstrap_seed, bootstrap_resamples, input_fingerprint)
        VALUES (?, ?, 1, '0.13.3-test', ?, 1, ?, ?, ?, ?, 5, 100, ?)
        """,
        (
            site_id,
            generation_id,
            state,
            json.dumps(snapshot, separators=(",", ":")),
            "2026-05-01",
            "2026-06-04",
            "2026-06-04",
            f"fp-read-cache-{_fp_counter}",
        ),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _wind_row(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    entity: tuple[str, str],
    date: str,
    predicted: float,
    lead: int = _LEAD,
) -> None:
    conn.execute(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, predicted, forecast_eligible,
             realized_contributors, truth_value, truth_eligible, abs_error)
        VALUES (?,?,?,?,'wind','wind_max',?,?,?,1,NULL,?,1,?)
        """,
        (
            run_id,
            date,
            date,
            lead,
            entity[0],
            entity[1],
            predicted,
            _WIND_TRUTH,
            abs(predicted - _WIND_TRUTH),
        ),
    )


def _wind_series(
    conn: sqlite3.Connection, run_id: int, entity: tuple[str, str], error: float
) -> None:
    for date in _WIND_DATES:
        _wind_row(conn, run_id, entity=entity, date=date, predicted=_WIND_TRUTH + error)


def _seed_wind(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    dominant_key: int = 1,
    dominant_error: float = 1.0,
    other_key: int = 2,
    other_error: float = 4.0,
) -> None:
    """Wind evidence yielding a W7 conclusion of ``indicated`` on ``dominant_key``."""
    for depth in SIM_DEPTHS:
        _wind_series(conn, run_id, ("depth", str(depth)), 4.0)
    _wind_series(
        conn, run_id, (DAILY_RANK_ENTITY_TYPE, str(dominant_key)), dominant_error
    )
    _wind_series(conn, run_id, (DAILY_RANK_ENTITY_TYPE, str(other_key)), other_error)


def _precip_row(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    entity: tuple[str, str],
    date: str,
    predicted: float,
    truth: float,
    wet_hours: int,
    lead: int = _LEAD,
) -> None:
    conn.execute(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, predicted, forecast_eligible,
             realized_contributors, truth_value, truth_eligible,
             truth_wet_hours, truth_dry_hours, abs_error)
        VALUES (?, ?, ?, ?, 'precip', 'precip_total', ?, ?, ?, 1, ?, ?, 1, ?, ?, ?)
        """,
        (
            run_id,
            date,
            date,
            lead,
            entity[0],
            entity[1],
            predicted,
            3,
            truth,
            wet_hours,
            23 - wet_hours,
            abs(predicted - truth),
        ),
    )


def _seed_precip(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    wet_errors: list[float] | None = None,
    dry_errors: list[float] | None = None,
) -> None:
    """Precip evidence with a wet-restricted mean distinguishable from all-days."""
    wet_errors = [1.0, 2.0, 3.0] * 3 if wet_errors is None else wet_errors
    dry_errors = [10.0] * 3 if dry_errors is None else dry_errors
    day = 1
    for error in wet_errors:
        date = f"2026-06-{day:02d}"
        day += 1
        for depth in SIM_DEPTHS:
            _precip_row(
                conn,
                run_id,
                entity=("depth", str(depth)),
                date=date,
                predicted=_WET_TRUTH + error,
                truth=_WET_TRUTH,
                wet_hours=2,
            )
    for error in dry_errors:
        date = f"2026-06-{day:02d}"
        day += 1
        for depth in SIM_DEPTHS:
            _precip_row(
                conn,
                run_id,
                entity=("depth", str(depth)),
                date=date,
                predicted=_DRY_TRUTH + error,
                truth=_DRY_TRUTH,
                wet_hours=0,
            )


def _seed_verdict(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    variable: str = "wind",
    outcome: str = "retain_incumbent",
    recommended_depth: int | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO verification_verdicts
            (run_id, variable, outcome, recommended_depth, incumbent_depth,
             tested_family)
        VALUES (?, ?, ?, ?, ?, '{}')
        """,
        (run_id, variable, outcome, recommended_depth, _DEPTH),
    )


def _full_run(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    state: str = "running",
    dominant_key: int = 1,
    other_key: int = 2,
) -> int:
    """One run carrying both W7 and W13 evidence, plus a verdict row."""
    run_id = _new_run(conn, site_id, state="running")
    _seed_wind(conn, run_id, dominant_key=dominant_key, other_key=other_key)
    _seed_precip(conn, run_id)
    _seed_verdict(conn, run_id)
    if state == "published":
        publish_run(conn, site_id, run_id)
    return run_id


async def _idle_worker(_db: object) -> None:  # pragma: no cover - never awaited
    await asyncio.Event().wait()


_CSRF_RE = re.compile(rb"[A-Za-z0-9_-]{20,}\.[0-9a-f]{40,}")


def _strip_csrf(body: bytes) -> bytes:
    """Every page embeds a fresh random CSRF token per request (api/csrf.py:
    ``nonce.hmac_hex``); it is unrelated to the read cache and must not
    defeat a byte-identity comparison across requests."""
    return _CSRF_RE.sub(b"REDACTED", body)


def _boot_app(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str = "cache.db"
) -> Any:
    from wxverify import config
    from wxverify.api.app import create_app
    from wxverify.db.connection import close_db, init_db

    close_db()
    config.db_path = str(tmp_path / name)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(config.db_path)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app: Any = create_app(root_path="")
    return db, app


# ---------------------------------------------------------------------------
# O1 - Equivalence
# ---------------------------------------------------------------------------


def test_o1_cached_equals_raw_for_both_derivations() -> None:
    conn = asof_conn()
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="published")
    conn.commit()

    cached_w7 = rc.cached_daily_rank_conclusions(conn, run_id)
    raw_w7 = daily_rank_conclusions(conn, run_id)
    assert cached_w7 == raw_w7
    assert json.dumps(cached_w7, sort_keys=True) == json.dumps(raw_w7, sort_keys=True)

    cached_w13 = rc.cached_observed_wet_precip_mae(conn, run_id)
    raw_w13 = observed_wet_precip_mae(conn, run_id)
    assert cached_w13 == raw_w13
    assert json.dumps(cached_w13, sort_keys=True) == json.dumps(raw_w13, sort_keys=True)


# ---------------------------------------------------------------------------
# O2 - Cold/warm request-level byte identity
# ---------------------------------------------------------------------------


def test_o2_cold_warm_byte_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.db.connection import close_db

    db, app = _boot_app(tmp_path, monkeypatch)
    conn = db._conn  # noqa: SLF001
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="published")
    conn.commit()
    try:
        with TestClient(app) as client:
            v1 = client.get(f"/api/verification/runs/{run_id}/verdicts")
            v2 = client.get(f"/api/verification/runs/{run_id}/verdicts")
            assert v1.status_code == 200
            assert v1.content == v2.content

            d1 = client.get(f"/api/verification/runs/{run_id}/diagnostics")
            d2 = client.get(f"/api/verification/runs/{run_id}/diagnostics")
            assert d1.status_code == 200
            assert d1.content == d2.content

            p1 = client.get(f"/verification?site={site_id}")
            p2 = client.get(f"/verification?site={site_id}")
            assert p1.status_code == 200
            assert _strip_csrf(p1.content) == _strip_csrf(p2.content)

            rc.reset_read_cache()
            v3 = client.get(f"/api/verification/runs/{run_id}/verdicts")
            d3 = client.get(f"/api/verification/runs/{run_id}/diagnostics")
            p3 = client.get(f"/verification?site={site_id}")
        assert v3.content == v1.content
        assert d3.content == d1.content
        assert _strip_csrf(p3.content) == _strip_csrf(p1.content)
    finally:
        close_db()


# ---------------------------------------------------------------------------
# O3 - Cross-surface agreement
# ---------------------------------------------------------------------------


def test_o3_page_context_and_verdicts_api_agree_under_the_cache() -> None:
    from wxverify.web.verification import load_verification

    conn = asof_conn()
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="published")
    conn.commit()

    context = load_verification(conn, site_id)
    verdicts = cast("list[dict[str, object]]", context["verdicts"])
    page_value = verdicts[0]["ranking_redesign_indicated"]
    api_value = rc.cached_daily_rank_conclusions(conn, run_id)["wind"]
    assert page_value == api_value


# ---------------------------------------------------------------------------
# O4 - Defensive copy
# ---------------------------------------------------------------------------


def test_o4_defensive_copy_isolates_stored_and_returned_objects() -> None:
    conn = asof_conn()
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="published")
    conn.commit()

    first = rc.cached_daily_rank_conclusions(conn, run_id)
    second = rc.cached_daily_rank_conclusions(conn, run_id)
    assert first is not second

    first["poison"] = "mutated"  # type: ignore[typeddict-item]
    first["wind"]["value"] = "mutated"

    third = rc.cached_daily_rank_conclusions(conn, run_id)
    assert "poison" not in third
    assert third["wind"]["value"] != "mutated"
    assert second["wind"]["value"] != "mutated"


# ---------------------------------------------------------------------------
# O5 - Engagement probe (kills M10)
# ---------------------------------------------------------------------------


def test_o5_engagement_probe_exactly_one_underlying_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = asof_conn()
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="published")
    conn.commit()

    calls = {"n": 0}
    real = rc.daily_rank_conclusions

    def _counting(
        conn: sqlite3.Connection, run_id: int, *, leads: tuple[int, ...] | None = None
    ) -> dict[str, dict[str, object]]:
        calls["n"] += 1
        return real(conn, run_id, leads=leads)

    monkeypatch.setattr(rc, "daily_rank_conclusions", _counting)
    rc.cached_daily_rank_conclusions(conn, run_id)
    rc.cached_daily_rank_conclusions(conn, run_id)
    assert calls["n"] == 1


# ---------------------------------------------------------------------------
# O6 - Structural cost oracle
# ---------------------------------------------------------------------------


def test_o6_second_read_issues_no_evidence_statements() -> None:
    conn = asof_conn()
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="published")
    conn.commit()
    rc.cached_daily_rank_conclusions(conn, run_id)  # warms the entry

    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        rc.cached_daily_rank_conclusions(conn, run_id)
    finally:
        conn.set_trace_callback(None)
    assert not any("verification_evidence" in s for s in statements)
    # The published-state gate runs on every call, by design.
    assert any("verification_runs" in s for s in statements)


# ---------------------------------------------------------------------------
# O7 - Published-only (kills M2 alongside O17)
# ---------------------------------------------------------------------------


def test_o7_only_a_published_run_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = asof_conn()
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="running")
    conn.commit()

    calls = {"n": 0}
    real = rc.daily_rank_conclusions

    def _counting(
        conn: sqlite3.Connection, run_id: int, *, leads: tuple[int, ...] | None = None
    ) -> dict[str, dict[str, object]]:
        calls["n"] += 1
        return real(conn, run_id, leads=leads)

    monkeypatch.setattr(rc, "daily_rank_conclusions", _counting)

    rc.cached_daily_rank_conclusions(conn, run_id)
    rc.cached_daily_rank_conclusions(conn, run_id)
    assert calls["n"] == 2  # never cached while running

    publish_run(conn, site_id, run_id)
    conn.commit()
    rc.cached_daily_rank_conclusions(conn, run_id)
    rc.cached_daily_rank_conclusions(conn, run_id)
    assert calls["n"] == 3  # one more underlying call, then a hit


# ---------------------------------------------------------------------------
# O8 - Generation invalidation
# ---------------------------------------------------------------------------


def test_o8_generation_bump_forces_rederivation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = asof_conn()
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="published")
    conn.commit()

    gen = {"n": 1}
    monkeypatch.setattr(rc, "current_db_generation", lambda: gen["n"])

    first = rc.cached_daily_rank_conclusions(conn, run_id)

    conn.execute(
        "UPDATE verification_evidence SET predicted = predicted + 100, "
        "abs_error = abs_error + 100 "
        "WHERE run_id = ? AND entity_type = ? AND entity_key = '1'",
        (run_id, DAILY_RANK_ENTITY_TYPE),
    )
    conn.commit()

    gen["n"] = 2
    second = rc.cached_daily_rank_conclusions(conn, run_id)
    assert second["wind"]["value"] != first["wind"]["value"]

    third = rc.cached_daily_rank_conclusions(conn, run_id)
    assert third == second  # post-bump value now served from the cache


# ---------------------------------------------------------------------------
# O9 - Single-flight (kills M11)
# ---------------------------------------------------------------------------


def test_o9_single_flight_two_threads_one_underlying_call() -> None:
    conn = _threaded_conn()
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="published")
    conn.commit()

    entered = threading.Event()
    release = threading.Event()
    calls = {"n": 0}
    calls_lock = threading.Lock()

    def _slow(conn: sqlite3.Connection, run_id: int) -> dict[str, object]:
        with calls_lock:
            calls["n"] += 1
        entered.set()
        assert release.wait(timeout=5.0)
        return {"wind": {"value": "indicated", "marker": "slow"}}

    results: list[object] = [None, None]
    errors: list[BaseException] = []

    def _call(i: int) -> None:
        try:
            results[i] = rc._cached(conn, run_id, rc._W7_NAME, _slow)  # noqa: SLF001
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=_call, args=(0,), daemon=True)
    t2 = threading.Thread(target=_call, args=(1,), daemon=True)
    t1.start()
    t2.start()
    try:
        assert entered.wait(timeout=5.0)
    finally:
        release.set()
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)
    assert not t1.is_alive()
    assert not t2.is_alive()
    assert not errors
    assert calls["n"] == 1
    assert results[0] == results[1]


# ---------------------------------------------------------------------------
# O10 - Tier separation, bounds, LRU order, sweep (kills M3)
# ---------------------------------------------------------------------------


def test_o10a_pinned_tier_survives_history_browsing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o10a.db"))
    conn = db._conn  # noqa: SLF001
    try:
        site_id = _site(conn)
        published_run = _full_run(conn, site_id, state="published")
        conn.commit()
        asyncio.run(rc.warm_read_cache(db))
        gen = rc.current_db_generation()
        assert (gen, rc._W7_NAME, published_run) in rc._PINNED

        calls = {"n": 0}
        real = rc.daily_rank_conclusions

        def _counting(
            conn: sqlite3.Connection,
            run_id: int,
            *,
            leads: tuple[int, ...] | None = None,
        ) -> dict[str, dict[str, object]]:
            calls["n"] += 1
            return real(conn, run_id, leads=leads)

        monkeypatch.setattr(rc, "daily_rank_conclusions", _counting)

        for i in range(rc._MAX_ENTRIES + 2):
            hist_site = _site(conn, name=f"site-hist-{i}")
            hist_run = _full_run(
                conn, hist_site, state="published", dominant_key=10 + i
            )
            conn.commit()
            rc.cached_daily_rank_conclusions(conn, hist_run)
            assert len(rc._ENTRIES) <= rc._MAX_ENTRIES
        assert calls["n"] == rc._MAX_ENTRIES + 2

        before = calls["n"]
        rc.cached_daily_rank_conclusions(conn, published_run)
        assert calls["n"] == before  # zero additional underlying calls
    finally:
        close_db()


def test_o10b_lru_order_within_the_lru_tier(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = asof_conn()
    site_id = _site(conn)
    calls = {"n": 0}
    real = rc.daily_rank_conclusions

    def _counting(
        conn: sqlite3.Connection, run_id: int, *, leads: tuple[int, ...] | None = None
    ) -> dict[str, dict[str, object]]:
        calls["n"] += 1
        return real(conn, run_id, leads=leads)

    monkeypatch.setattr(rc, "daily_rank_conclusions", _counting)

    runs: list[int] = []
    for i in range(rc._MAX_ENTRIES):
        r = _full_run(conn, site_id, state="published", dominant_key=30 + i)
        conn.commit()
        runs.append(r)
        rc.cached_daily_rank_conclusions(conn, r)
    assert calls["n"] == rc._MAX_ENTRIES
    assert len(rc._ENTRIES) <= rc._MAX_ENTRIES
    assert rc._PINNED == {}  # no request-path read ever inserts into _PINNED

    before = calls["n"]
    rc.cached_daily_rank_conclusions(conn, runs[0])  # touch the earliest -> MRU
    assert calls["n"] == before

    extra = _full_run(conn, site_id, state="published", dominant_key=99)
    conn.commit()
    before = calls["n"]
    rc.cached_daily_rank_conclusions(conn, extra)  # tier full: evicts runs[1]
    assert calls["n"] == before + 1
    assert len(rc._ENTRIES) <= rc._MAX_ENTRIES

    before = calls["n"]
    rc.cached_daily_rank_conclusions(conn, runs[0])
    assert calls["n"] == before  # still a hit

    before = calls["n"]
    rc.cached_daily_rank_conclusions(conn, runs[1])
    assert calls["n"] == before + 1  # miss: evicted


def test_o10d_generation_bump_sweeps_both_tiers_on_next_miss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o10d.db"))
    conn = db._conn  # noqa: SLF001
    try:
        site_id = _site(conn)
        run_id = _full_run(conn, site_id, state="published")
        conn.commit()
        asyncio.run(rc.warm_read_cache(db))
        real_gen = rc.current_db_generation()
        stale_pinned_key = (real_gen, rc._W7_NAME, run_id)
        assert stale_pinned_key in rc._PINNED

        other = _full_run(conn, site_id, state="published", dominant_key=55)
        conn.commit()
        rc.cached_daily_rank_conclusions(conn, other)
        stale_entries_key = (real_gen, rc._W7_NAME, other)
        assert stale_entries_key in rc._ENTRIES

        monkeypatch.setattr(rc, "current_db_generation", lambda: 999)
        rc.cached_daily_rank_conclusions(conn, run_id)  # any miss triggers the sweep
        assert stale_pinned_key not in rc._PINNED
        assert stale_entries_key not in rc._ENTRIES
        assert all(k[0] != real_gen for k in rc._PINNED)
        assert all(k[0] != real_gen for k in rc._ENTRIES)
    finally:
        close_db()


# ---------------------------------------------------------------------------
# O11a - Warm populates, pins, is idempotent
# ---------------------------------------------------------------------------


def test_o11a_warm_populates_pins_and_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o11a.db"))
    conn = db._conn  # noqa: SLF001
    try:
        site_id = _site(conn)
        run_id = _full_run(conn, site_id, state="published")
        conn.commit()

        calls = {"n": 0}
        real_w7 = rc.daily_rank_conclusions
        real_w13 = rc.observed_wet_precip_mae

        def _c_w7(
            conn: sqlite3.Connection,
            run_id: int,
            *,
            leads: tuple[int, ...] | None = None,
        ) -> dict[str, dict[str, object]]:
            calls["n"] += 1
            return real_w7(conn, run_id, leads=leads)

        def _c_w13(conn: sqlite3.Connection, run_id: int) -> dict[str, object]:
            calls["n"] += 1
            return real_w13(conn, run_id)

        monkeypatch.setattr(rc, "daily_rank_conclusions", _c_w7)
        monkeypatch.setattr(rc, "observed_wet_precip_mae", _c_w13)

        asyncio.run(rc.warm_read_cache(db))
        assert calls["n"] == 2  # one W7 call, one W13 call
        gen = rc.current_db_generation()
        assert (gen, rc._W7_NAME, run_id) in rc._PINNED
        assert (gen, rc._W13_NAME, run_id) in rc._PINNED
        assert (gen, rc._W7_NAME, run_id) not in rc._ENTRIES
        assert (gen, rc._W13_NAME, run_id) not in rc._ENTRIES

        before = calls["n"]
        rc.cached_daily_rank_conclusions(conn, run_id)
        rc.cached_observed_wet_precip_mae(conn, run_id)
        assert calls["n"] == before  # served from the pinned tier

        before = calls["n"]
        asyncio.run(rc.warm_read_cache(db))
        assert calls["n"] == before  # idempotent
    finally:
        close_db()


def test_o11a_warm_skips_a_running_or_missing_published_pointer(
    tmp_path: Path,
) -> None:
    """The §4.22 partial-import shape: a pointer to a non-cacheable target."""
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o11a-partial.db"))
    conn = db._conn  # noqa: SLF001
    try:
        site_running = _site(conn, name="site-beta")
        run_running = _full_run(conn, site_running, state="running")
        set_runtime_state(conn, published_run_key(site_running), str(run_running))

        site_missing = _site(conn, name="site-gamma")
        set_runtime_state(conn, published_run_key(site_missing), "999999")
        conn.commit()

        asyncio.run(rc.warm_read_cache(db))  # must return normally, no crash

        assert not any(k[2] == run_running for k in rc._PINNED)
        assert not any(k[2] == run_running for k in rc._ENTRIES)
        assert not any(k[2] == 999999 for k in rc._PINNED)  # noqa: PLR2004
        assert not any(k[2] == 999999 for k in rc._ENTRIES)  # noqa: PLR2004
    finally:
        close_db()


# ---------------------------------------------------------------------------
# O11b - Warm failure stores nothing, per (site, derivation) scope
# (kills M5)
# ---------------------------------------------------------------------------


def test_o11b_i_derivation_failure_stores_nothing_and_the_next_read_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o11b1.db"))
    conn = db._conn  # noqa: SLF001
    try:
        site_id = _site(conn)
        run_id = _full_run(conn, site_id, state="published")
        conn.commit()

        calls = {"n": 0}

        def _raising(conn: sqlite3.Connection, run_id: int, **_kw: object) -> object:
            calls["n"] += 1
            raise RuntimeError("derivation exploded")

        monkeypatch.setattr(rc, "daily_rank_conclusions", _raising)
        asyncio.run(rc.warm_read_cache(db))  # must not raise
        gen = rc.current_db_generation()
        assert (gen, rc._W7_NAME, run_id) not in rc._PINNED
        assert (gen, rc._W7_NAME, run_id) not in rc._ENTRIES
        assert calls["n"] == 1

        with pytest.raises(RuntimeError):
            rc.cached_daily_rank_conclusions(conn, run_id)
        assert calls["n"] == 2  # a later request-path read calls it again
    finally:
        close_db()


def test_o11b_ii_deepcopy_failure_stores_nothing_and_next_read_raises_fresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = asof_conn()
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="published")
    conn.commit()

    calls = {"n": 0}

    def _uncopyable(conn: sqlite3.Connection, run_id: int, **_kw: object) -> object:
        calls["n"] += 1
        return {"row": conn.execute("SELECT 1 AS x").fetchone()}  # sqlite3.Row leaks

    monkeypatch.setattr(rc, "daily_rank_conclusions", _uncopyable)

    with pytest.raises(TypeError):
        rc.cached_daily_rank_conclusions(conn, run_id)
    assert calls["n"] == 1
    gen = rc.current_db_generation()
    assert (gen, rc._W7_NAME, run_id) not in rc._PINNED
    assert (gen, rc._W7_NAME, run_id) not in rc._ENTRIES

    with pytest.raises(TypeError):
        rc.cached_daily_rank_conclusions(conn, run_id)
    assert calls["n"] == 2  # raised from a fresh derivation, not a stored value


def test_o11b_iii_one_sites_failure_does_not_suppress_a_later_sites_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o11b3.db"))
    conn = db._conn  # noqa: SLF001
    try:
        site_a = _site(conn, name="site-alpha")
        run_a = _full_run(conn, site_a, state="published")
        site_b = _site(conn, name="site-beta")
        run_b = _full_run(conn, site_b, state="published", dominant_key=7)
        conn.commit()

        real = rc.daily_rank_conclusions

        def _selective(
            conn: sqlite3.Connection,
            run_id: int,
            *,
            leads: tuple[int, ...] | None = None,
        ) -> dict[str, dict[str, object]]:
            if run_id == run_a:
                raise RuntimeError("site A derivation failed")
            return real(conn, run_id, leads=leads)

        monkeypatch.setattr(rc, "daily_rank_conclusions", _selective)
        asyncio.run(rc.warm_read_cache(db))

        gen = rc.current_db_generation()
        assert (gen, rc._W7_NAME, run_a) not in rc._PINNED
        assert (gen, rc._W7_NAME, run_a) not in rc._ENTRIES
        assert (gen, rc._W7_NAME, run_b) in rc._PINNED
        assert (gen, rc._W13_NAME, run_b) in rc._PINNED
    finally:
        close_db()


# ---------------------------------------------------------------------------
# O11c - Warm's total catch covers the whole frame
# ---------------------------------------------------------------------------


def test_o11c_warm_catch_covers_the_pointer_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o11c1.db"))
    try:

        def _raising(conn: sqlite3.Connection) -> list[tuple[int, int]]:
            raise RuntimeError("published_targets exploded")

        monkeypatch.setattr(rc, "_published_targets", _raising)
        with caplog.at_level(logging.ERROR, logger="wxverify.verification.read_cache"):
            result = asyncio.run(rc.warm_read_cache(db))
        assert result is None
        assert any(r.levelno == logging.ERROR for r in caplog.records)
    finally:
        close_db()


def test_o11c_warm_catch_covers_db_read_itself(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from wxverify.db.connection import Database, close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o11c2.db"))

    async def _raising_read(fn: object) -> object:
        raise RuntimeError("db.read exploded")

    try:
        monkeypatch.setattr(Database, "read", lambda self, fn: _raising_read(fn))
        with caplog.at_level(logging.ERROR, logger="wxverify.verification.read_cache"):
            result = asyncio.run(rc.warm_read_cache(db))
        assert result is None
        assert any(r.levelno == logging.ERROR for r in caplog.records)
    finally:
        close_db()


# ---------------------------------------------------------------------------
# O12 - `leads` unrepresentable
# ---------------------------------------------------------------------------


def test_o12_leads_is_unrepresentable_at_the_cached_entry_point() -> None:
    assert "leads" not in inspect.signature(rc.cached_daily_rank_conclusions).parameters
    assert "leads" in inspect.signature(daily_rank_conclusions).parameters


# ---------------------------------------------------------------------------
# O13 - Per-run isolation (kills M1)
# ---------------------------------------------------------------------------


def test_o13_per_run_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = asof_conn()
    site_a = _site(conn, name="site-alpha")
    site_b = _site(conn, name="site-beta")
    run_a = _full_run(conn, site_a, state="published", dominant_key=1, other_key=2)
    run_b = _full_run(conn, site_b, state="published", dominant_key=3, other_key=4)
    conn.commit()

    calls = {"n": 0}
    real = rc.daily_rank_conclusions

    def _counting(
        conn: sqlite3.Connection, run_id: int, *, leads: tuple[int, ...] | None = None
    ) -> dict[str, dict[str, object]]:
        calls["n"] += 1
        return real(conn, run_id, leads=leads)

    monkeypatch.setattr(rc, "daily_rank_conclusions", _counting)

    a1 = rc.cached_daily_rank_conclusions(conn, run_a)
    b1 = rc.cached_daily_rank_conclusions(conn, run_b)
    a2 = rc.cached_daily_rank_conclusions(conn, run_a)
    b2 = rc.cached_daily_rank_conclusions(conn, run_b)

    assert a1 == a2
    assert b1 == b2
    assert a1["wind"]["selected_entity_key"] != b1["wind"]["selected_entity_key"]
    assert a1 != b1
    assert calls["n"] == 2


# ---------------------------------------------------------------------------
# O14 - Cancellation is not caught (kills M7)
# ---------------------------------------------------------------------------


def test_o14_cancellation_of_warm_is_not_caught(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o14.db"))
    try:
        site_a = _site(conn := db._conn, name="site-alpha")  # noqa: SLF001
        run_a = _full_run(conn, site_a, state="published")
        site_b = _site(conn, name="site-beta")
        run_b = _full_run(conn, site_b, state="published", dominant_key=7)
        conn.commit()

        gate = threading.Event()
        a_done = threading.Event()
        real = rc.daily_rank_conclusions

        def _blocking(
            conn: sqlite3.Connection,
            run_id: int,
            *,
            leads: tuple[int, ...] | None = None,
        ) -> dict[str, dict[str, object]]:
            if run_id == run_b:
                assert gate.wait(timeout=5.0)
                return real(conn, run_id, leads=leads)
            result = real(conn, run_id, leads=leads)
            a_done.set()
            return result

        # monkeypatch without pytest fixture: manual restore in finally
        original = rc.daily_rank_conclusions
        rc.daily_rank_conclusions = _blocking  # type: ignore[assignment]
        try:

            async def _run() -> None:
                task = asyncio.create_task(rc.warm_read_cache(db))
                # Rendezvous, not a sleep: wait for site A's warm to have
                # genuinely finished before cancelling.
                await asyncio.wait_for(asyncio.to_thread(a_done.wait, 5.0), timeout=5.0)
                task.cancel()
                try:
                    gate.set()
                    with caplog.at_level(
                        logging.ERROR, logger="wxverify.verification.read_cache"
                    ):
                        await asyncio.wait({task}, timeout=5.0)
                finally:
                    gate.set()
                assert task.done()
                assert task.cancelled()

            asyncio.run(_run())
        finally:
            rc.daily_rank_conclusions = original  # type: ignore[assignment]

        assert not any(r.levelno == logging.ERROR for r in caplog.records)
        gen = rc.current_db_generation()
        assert (gen, rc._W7_NAME, run_a) in rc._ENTRIES
        assert rc._PINNED == {}
    finally:
        close_db()


# ---------------------------------------------------------------------------
# O15 - Every lock released on the raise path (kills M6)
# ---------------------------------------------------------------------------


def test_o15_stripe_lock_released_when_the_derivation_raises() -> None:
    conn = _threaded_conn()
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="published")
    conn.commit()

    def _raising(conn: sqlite3.Connection, run_id: int, **_kw: object) -> object:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        rc._cached(conn, run_id, rc._W7_NAME, _raising)  # noqa: SLF001

    results: list[object] = [None]
    errors: list[BaseException] = []

    def _call() -> None:
        try:
            results[0] = rc._cached(conn, run_id, rc._W7_NAME, daily_rank_conclusions)  # noqa: SLF001
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t = threading.Thread(target=_call, daemon=True)
    t.start()
    t.join(timeout=5.0)
    assert not t.is_alive()
    assert not errors
    assert results[0] is not None


# ---------------------------------------------------------------------------
# O16 - The pinned tier is reconciled, not accreted (kills M4)
# ---------------------------------------------------------------------------


def test_o16_pinned_tier_is_reconciled_not_accreted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o16.db"))
    conn = db._conn  # noqa: SLF001
    try:
        site_id = _site(conn)
        original_run = _full_run(conn, site_id, state="published")
        conn.commit()
        asyncio.run(rc.warm_read_cache(db))
        first_size = len(rc._PINNED)
        real_gen = rc.current_db_generation()

        monkeypatch.setattr(rc, "current_db_generation", lambda: real_gen + 1)
        asyncio.run(rc.warm_read_cache(db))
        assert len(rc._PINNED) == first_size
        assert all(k[0] == real_gen + 1 for k in rc._PINNED)

        another_site = _site(conn, name="site-beta")
        _full_run(conn, another_site, state="published", dominant_key=9)
        conn.commit()
        monkeypatch.setattr(rc, "current_db_generation", lambda: real_gen + 2)
        asyncio.run(rc.warm_read_cache(db))
        assert len(rc._PINNED) == first_size * 2
        assert all(k[0] == real_gen + 2 for k in rc._PINNED)

        # Same-generation supersession: `_sweep_stale` (inside `_insert`)
        # already drops any stale-GENERATION pin, so the two rounds above
        # cannot distinguish "replace" from "accrete" -- the old round's
        # entries are gone by the time reconciliation runs regardless of
        # which one `_reconcile_pins` performs. Publish a SECOND run for
        # `site_id` at the SAME generation (real_gen + 2, unchanged) so the
        # only thing that can drop the first run's pins is reconciliation
        # itself replacing the pinned tier, not the generation sweep.
        superseded_run = original_run
        assert any(k[2] == superseded_run for k in rc._PINNED)
        new_run = _full_run(
            conn, site_id, state="published", dominant_key=1, other_key=2
        )
        assert new_run != superseded_run
        conn.commit()
        asyncio.run(rc.warm_read_cache(db))
        assert len(rc._PINNED) == first_size * 2
        assert not any(k[2] == superseded_run for k in rc._PINNED), (
            "the superseded run's pins must be REPLACED, not retained"
        )
        assert any(k[2] == new_run for k in rc._PINNED)
    finally:
        close_db()


# ---------------------------------------------------------------------------
# O17 - The published gate covers the lookup, not only the store (kills M2)
# ---------------------------------------------------------------------------


def test_o17_published_gate_covers_the_lookup() -> None:
    conn = asof_conn()
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="published")
    conn.commit()

    first = rc.cached_daily_rank_conclusions(conn, run_id)
    assert first is not None

    conn.execute(
        "UPDATE verification_runs SET state = 'running' WHERE id = ?", (run_id,)
    )
    conn.execute(
        "UPDATE verification_evidence SET predicted = predicted + 100, "
        "abs_error = abs_error + 100 "
        "WHERE run_id = ? AND entity_type = ? AND entity_key = '1'",
        (run_id, DAILY_RANK_ENTITY_TYPE),
    )
    conn.commit()

    calls = {"n": 0}
    real = rc.daily_rank_conclusions

    def _counting(
        conn: sqlite3.Connection, run_id: int, *, leads: tuple[int, ...] | None = None
    ) -> dict[str, dict[str, object]]:
        calls["n"] += 1
        return real(conn, run_id, leads=leads)

    import wxverify.verification.read_cache as rc_mod

    old = rc_mod.daily_rank_conclusions
    rc_mod.daily_rank_conclusions = _counting  # type: ignore[assignment]
    try:
        second = rc.cached_daily_rank_conclusions(conn, run_id)
    finally:
        rc_mod.daily_rank_conclusions = old  # type: ignore[assignment]

    assert calls["n"] == 1  # a second underlying call happened
    assert second["wind"]["value"] != first["wind"]["value"]


# ---------------------------------------------------------------------------
# O18 - The warm stages its own results (kills M8)
# ---------------------------------------------------------------------------


def test_o18_warm_stages_its_own_results_not_read_back_from_entries(
    tmp_path: Path,
) -> None:
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o18.db"))
    conn = db._conn  # noqa: SLF001
    try:
        assert rc._MAX_ENTRIES == 4  # noqa: PLR2004
        site_ids = []
        run_ids = []
        for i in range(3):
            site_id = _site(conn, name=f"site-third-{i}")
            run_id = _full_run(conn, site_id, state="published", dominant_key=40 + i)
            site_ids.append(site_id)
            run_ids.append(run_id)
        conn.commit()

        calls = {"n": 0}
        real_w7 = rc.daily_rank_conclusions
        real_w13 = rc.observed_wet_precip_mae

        def _c_w7(
            conn: sqlite3.Connection,
            run_id: int,
            *,
            leads: tuple[int, ...] | None = None,
        ) -> dict[str, dict[str, object]]:
            calls["n"] += 1
            return real_w7(conn, run_id, leads=leads)

        def _c_w13(conn: sqlite3.Connection, run_id: int) -> dict[str, object]:
            calls["n"] += 1
            return real_w13(conn, run_id)

        import wxverify.verification.read_cache as rc_mod

        rc_mod.daily_rank_conclusions = _c_w7  # type: ignore[assignment]
        rc_mod.observed_wet_precip_mae = _c_w13  # type: ignore[assignment]
        try:
            asyncio.run(rc.warm_read_cache(db))
        finally:
            rc_mod.daily_rank_conclusions = real_w7  # type: ignore[assignment]
            rc_mod.observed_wet_precip_mae = real_w13  # type: ignore[assignment]

        assert calls["n"] == 6  # 3 sites x 2 derivations
        gen = rc.current_db_generation()
        expected = {(gen, rc._W7_NAME, r) for r in run_ids} | {
            (gen, rc._W13_NAME, r) for r in run_ids
        }
        assert set(rc._PINNED.keys()) == expected
        assert not (set(rc._ENTRIES.keys()) & expected)
        assert len(rc._ENTRIES) <= rc._MAX_ENTRIES

        before = calls["n"]
        for r in run_ids:
            rc.cached_daily_rank_conclusions(conn, r)
            rc.cached_observed_wet_precip_mae(conn, r)
        assert calls["n"] == before  # zero underlying calls: all pinned

        fourth_site = _site(conn, name="site-third-nopointer")
        _full_run(conn, fourth_site, state="running")  # no publish -> no pointer
        conn.commit()
        before = calls["n"]
        asyncio.run(rc.warm_read_cache(db))
        assert calls["n"] == before  # idempotent, fourth site costs nothing
        assert set(rc._PINNED.keys()) == expected
    finally:
        close_db()


# ---------------------------------------------------------------------------
# O19 - The publish-time leg fires once, and after the write
#
# These drive the REAL sync chain (discover -> regen -> decide -> start ->
# simulate -> resolve -> baseline -> aggregate -> bootstrap -> pairwise ->
# publish) against a migrated database, mirroring the house idiom in
# test_verification_no_change_oracles.py, so that `publish_verified_run`'s
# integrity check (verdicts/results rows) is satisfied by a genuinely
# completed run rather than a hand-faked state blob.
# ---------------------------------------------------------------------------

_O19_PERIOD_DAYS = [
    "2026-06-01",
    "2026-06-02",
    "2026-06-03",
    "2026-06-04",
    "2026-06-05",
]
_O19_QUANTITY_VALUES = {
    "temperature_high": 21.0,
    "temperature_low": 9.0,
    "wind_max": 6.0,
    "precip_total": 0.0,
    "precip_occurrence": 0.0,
}


def _o19_make_site(conn: sqlite3.Connection, name: str = "oracle-town") -> int:
    from datetime import UTC as _UTC
    from datetime import datetime as _dt
    from datetime import timedelta as _td

    from tests.helpers import asof_make_real_feed
    from wxverify.db.tz_generations import ensure_published_generation

    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (?, 40.0, -105.0, 900.0, 'UTC')
        """,
        (name,),
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    feeds = [
        asof_make_real_feed(conn, "model-gamma"),
        asof_make_real_feed(conn, "model-delta"),
    ]
    generation_id = ensure_published_generation(conn, site_id)
    for day in _O19_PERIOD_DAYS:
        for quantity, value in _O19_QUANTITY_VALUES.items():
            conn.execute(
                """
                INSERT INTO daily_truth
                    (site_id, local_date, quantity, value, eligible,
                     covered_hours, expected_slots, wet_hours, dry_hours,
                     rain_threshold_mm, day_start_utc, day_end_utc, timezone,
                     tz_generation_id)
                VALUES (?, ?, ?, ?, 1, 24, 24, ?, ?, 0.2, ?, ?, 'UTC', ?)
                """,
                (
                    site_id,
                    day,
                    quantity,
                    value,
                    0 if quantity.startswith("precip") else None,
                    24 if quantity.startswith("precip") else None,
                    f"{day}T00:00:00Z",
                    f"{day}T23:59:59Z",
                    generation_id,
                ),
            )
    values = {"temperature": 15.0, "wind": 5.0, "precip": 0.0}
    issued = "2026-05-31T05:00:00Z"
    for feed_index, feed_id in enumerate(feeds):
        for variable, base in values.items():
            for day_offset in range(len(_O19_PERIOD_DAYS) + 8):
                for hour in range(24):
                    total_hours = day_offset * 24 + hour
                    valid = (
                        _dt(2026, 6, 1, tzinfo=_UTC) + _td(hours=total_hours)
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
                    conn.execute(
                        """
                        INSERT INTO forecast_samples
                            (site_id, feed_id, variable, issued_at, valid_at,
                             lead_hours, value, source_raw, model_run_id, fetched_at)
                        VALUES (?, ?, ?, ?, ?, 6, ?, '{}', 'run-a', ?)
                        """,
                        (
                            site_id,
                            feed_id,
                            variable,
                            issued,
                            valid,
                            base + 0.5 * feed_index,
                            issued,
                        ),
                    )
    conn.commit()
    return site_id


def _o19_drive_until_phase(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    phase: str,
    *,
    resamples: int = 40,
    max_steps: int = 300,
) -> None:
    """Sync-drive the chain up to (not including) the named phase's step."""
    from wxverify.verification.engine import prepare_bootstrap_inputs
    from wxverify.verification.runs import run_config_from_row
    from wxverify.worker.verification_run import (
        _compute_verdicts,  # noqa: SLF001
        _load_state,  # noqa: SLF001
        _persist_verdicts,  # noqa: SLF001
        advance_verification,
    )

    for _ in range(max_steps):
        blob = _load_state(conn, site_id)
        if blob is not None and blob.get("phase") == phase:
            return
        if blob is not None and blob.get("phase") == "bootstrap":
            run_id = blob["run_id"]
            assert isinstance(run_id, int)
            cfg = run_config_from_row(conn, run_id)
            inputs = prepare_bootstrap_inputs(conn, cfg)
            verdicts = _compute_verdicts(inputs, cfg.bootstrap_seed, resamples)
            _persist_verdicts(conn, site_id, cfg, verdicts)
            continue
        assert advance_verification(conn, site_id, payload)
    raise AssertionError(f"chain never reached phase {phase!r}")


async def _o19_drive_all_chunks(
    db: Any,
    writer: Any,
    site_id: int,
    payload: dict[str, object],
    *,
    max_chunks: int = 300,
) -> None:
    """Async-drive the chain (via the real orchestrator entry point) to
    completion, including any real warm_read_cache call it fires."""
    from wxverify.worker.verification_run import run_verification_chunk

    for _ in range(max_chunks):
        result = await run_verification_chunk(db, writer, site_id, payload)
        if result is None:
            return
    raise AssertionError("chain did not terminate")


def test_o19_publish_time_warm_fires_once_after_the_pointer_flips(
    tmp_path: Path,
) -> None:
    from wxverify.db.connection import FencedWriter, close_db, init_db
    from wxverify.verification.runs import published_run_id
    from wxverify.worker.verification_run import (
        _load_state,  # noqa: SLF001
        run_verification_chunk,
    )

    close_db()
    db = init_db(str(tmp_path / "o19a.db"))
    try:
        conn = db._conn  # noqa: SLF001
        site_id = _o19_make_site(conn)
        payload: dict[str, object] = {"trigger_date": "2026-06-06"}
        _o19_drive_until_phase(conn, site_id, payload, "publish")
        conn.commit()
        assert published_run_id(conn, site_id) is None  # not yet published
        blob = _load_state(conn, site_id)
        assert blob is not None
        target_run = blob["run_id"]

        recorder: dict[str, object] = {}

        async def _recording_warm(db: object) -> None:
            recorder["published_run_id"] = published_run_id(
                cast(Any, db)._conn,
                site_id,  # noqa: SLF001
            )
            recorder["calls"] = cast(int, recorder.get("calls", 0)) + 1

        import wxverify.worker.verification_run as vr_mod

        original = vr_mod.warm_read_cache
        vr_mod.warm_read_cache = _recording_warm  # type: ignore[assignment]
        try:
            writer = FencedWriter(db, db.generation)

            async def _drive() -> object | None:
                return await run_verification_chunk(db, writer, site_id, payload)

            result = asyncio.run(_drive())
        finally:
            vr_mod.warm_read_cache = original  # type: ignore[assignment]

        assert result is None  # terminal
        assert recorder["calls"] == 1
        assert recorder["published_run_id"] == target_run
    finally:
        close_db()


def test_o19_skip_path_reaches_the_terminal_branch_with_zero_derivations(
    tmp_path: Path,
) -> None:
    from wxverify.db.connection import FencedWriter, close_db, init_db
    from wxverify.verification.runs import published_run_id
    from wxverify.worker.verification_run import (
        _load_state,  # noqa: SLF001
        verification_state_key,
    )

    close_db()
    db = init_db(str(tmp_path / "o19b.db"))
    try:
        conn = db._conn  # noqa: SLF001
        site_id = _o19_make_site(conn)
        payload: dict[str, object] = {"trigger_date": "2026-06-06"}
        writer = FencedWriter(db, db.generation)
        asyncio.run(_o19_drive_all_chunks(db, writer, site_id, payload))
        run1 = published_run_id(conn, site_id)
        assert run1 is not None  # first night's run really published

        calls = {"n": 0}

        async def _counting_warm(db: object) -> None:
            calls["n"] += 1

        import wxverify.verification.read_cache as rc_mod
        import wxverify.worker.verification_run as vr_mod

        rc_calls = {"n": 0}
        real_w7 = rc_mod.daily_rank_conclusions
        real_w13 = rc_mod.observed_wet_precip_mae

        def _c_w7(
            conn: sqlite3.Connection,
            run_id: int,
            *,
            leads: tuple[int, ...] | None = None,
        ) -> dict[str, dict[str, object]]:
            rc_calls["n"] += 1
            return real_w7(conn, run_id, leads=leads)

        def _c_w13(conn: sqlite3.Connection, run_id: int) -> dict[str, object]:
            rc_calls["n"] += 1
            return real_w13(conn, run_id)

        rc_mod.daily_rank_conclusions = _c_w7  # type: ignore[assignment]
        rc_mod.observed_wet_precip_mae = _c_w13  # type: ignore[assignment]
        original = vr_mod.warm_read_cache
        vr_mod.warm_read_cache = _counting_warm  # type: ignore[assignment]
        try:
            # Same fixture, unchanged inputs, a second trigger date: the
            # `_decide_phase` no-change gate fires on the very first
            # "decide"-phase chunk it reaches (`_blocking_gate` in
            # verification_run.py), well before any derivation.
            payload2: dict[str, object] = {"trigger_date": "2026-06-07"}
            asyncio.run(_o19_drive_all_chunks(db, writer, site_id, payload2))
        finally:
            vr_mod.warm_read_cache = original  # type: ignore[assignment]
            rc_mod.daily_rank_conclusions = real_w7  # type: ignore[assignment]
            rc_mod.observed_wet_precip_mae = real_w13  # type: ignore[assignment]

        assert calls["n"] == 1  # the terminal branch fires exactly once
        assert rc_calls["n"] == 0  # and zero derivations for a no-op decision
        assert published_run_id(conn, site_id) == run1  # pointer unchanged
        assert _load_state(conn, site_id) is None  # blob cleared
        assert (
            conn.execute(
                "SELECT value FROM runtime_state WHERE key = ?",
                (verification_state_key(site_id),),
            ).fetchone()
            is None
        )
    finally:
        close_db()


def test_o19_cancellation_leaves_the_publish_committed_and_the_job_running(
    tmp_path: Path,
) -> None:
    from wxverify.db.connection import FencedWriter, close_db, init_db
    from wxverify.verification.runs import published_run_id
    from wxverify.worker.verification_run import (
        _load_state,  # noqa: SLF001
        run_verification_chunk,
        verification_state_key,
    )

    close_db()
    db = init_db(str(tmp_path / "o19c.db"))
    try:
        conn = db._conn  # noqa: SLF001
        site_id = _o19_make_site(conn)
        payload: dict[str, object] = {"trigger_date": "2026-06-06"}
        _o19_drive_until_phase(conn, site_id, payload, "publish")
        conn.commit()
        assert published_run_id(conn, site_id) is None
        blob = _load_state(conn, site_id)
        assert blob is not None
        target_run = blob["run_id"]

        gate = threading.Event()
        entered = asyncio.Event()

        async def _blocking_warm(db: object) -> None:
            entered.set()
            await asyncio.to_thread(gate.wait, 5.0)

        import wxverify.worker.verification_run as vr_mod

        original = vr_mod.warm_read_cache
        vr_mod.warm_read_cache = _blocking_warm  # type: ignore[assignment]
        try:
            writer = FencedWriter(db, db.generation)

            async def _run() -> None:
                task = asyncio.create_task(
                    run_verification_chunk(db, writer, site_id, payload)
                )
                # Rendezvous, not a sleep: wait for `run_verification_chunk`
                # to have actually reached `warm_read_cache` (i.e. the
                # publish already committed) before cancelling.
                await asyncio.wait_for(entered.wait(), timeout=5.0)
                task.cancel()
                try:
                    gate.set()
                    await asyncio.wait({task}, timeout=5.0)
                finally:
                    gate.set()
                assert task.cancelled()

            asyncio.run(_run())
        finally:
            vr_mod.warm_read_cache = original  # type: ignore[assignment]

        assert published_run_id(conn, site_id) == target_run  # the publish committed
        assert _load_state(conn, site_id) is None  # _clear_state already ran
        blob_row = conn.execute(
            "SELECT value FROM runtime_state WHERE key = ?",
            (verification_state_key(site_id),),
        ).fetchone()
        assert blob_row is None
        # The publish write and its `_clear_state` committed BEFORE the
        # cancel landed (`warm_read_cache` runs strictly after `writer.write`
        # returns); only the cache warm itself was interrupted.
    finally:
        close_db()


# ---------------------------------------------------------------------------
# O20 - A failing warm cannot break shutdown, and cannot fail silently
# (kills M14)
# ---------------------------------------------------------------------------


def test_o20_a_failing_warm_does_not_break_shutdown_and_is_logged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    db, app = _boot_app(tmp_path, monkeypatch, name="o20.db")

    async def _raising_warm(db: object) -> None:
        raise RuntimeError("warm exploded")

    monkeypatch.setattr("wxverify.api.app.warm_read_cache", _raising_warm)
    from wxverify.db.connection import close_db

    try:
        with caplog.at_level(logging.INFO), TestClient(app) as client:
            resp = client.get("/api/health/monitor")
            assert resp.status_code == 200
        assert any("worker stopping" in r.getMessage() for r in caplog.records)
        # `exc_info is not None` alone does not discriminate: a
        # `logger.exception(...)` call with no active exception still stamps
        # `(None, None, None)`, which is "not None". Require the record to
        # carry the actual exception instance the warm raised.
        error_records = [
            r
            for r in caplog.records
            if r.levelno == logging.ERROR
            and r.exc_info is not None
            and isinstance(r.exc_info[1], RuntimeError)
            and str(r.exc_info[1]) == "warm exploded"
        ]
        assert any("read-cache warm failed" in r.getMessage() for r in error_records)
    finally:
        close_db()


# ---------------------------------------------------------------------------
# O20b - Reaping is collective, not per-task: the "warm failed" outcome
# cannot be logged while a sibling task is still mid-cancellation
# ---------------------------------------------------------------------------


def test_o20b_warm_is_awaited_after_worker_on_shutdown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # `_cancel_and_reap` now cancels every registered task in one
    # non-yielding pass, then does a SINGLE
    # `await asyncio.gather(*handles, return_exceptions=True)` over all of
    # them -- there is no longer a per-task `.cancel()` / `await` pair to
    # transpose, so a completion-order mutant on individual cancel/await
    # statements no longer applies. What this test still pins: `gather`
    # cannot return until every child task is actually done, so the
    # "warm failed" log (emitted only after `gather` returns) cannot appear
    # while a sibling task is still blocked mid-cancellation.
    #
    # The genuinely order-sensitive construction: make `warm`'s task ALREADY
    # DONE (holding a stored exception) before shutdown ever starts, so its
    # result is available in `gather`'s return list immediately. Make
    # `worker`'s cancellation handling block on a gate only this test
    # releases, so `gather` cannot resolve yet. Run shutdown in a background
    # thread so the main thread can observe mid-shutdown state: while the
    # worker is still gated, the "warm failed" log must not have appeared
    # yet, because `gather` -- and therefore the whole reap loop that logs
    # it -- has not returned.
    from wxverify.db.connection import close_db

    async def _warm_stub(db: object) -> None:
        # No internal await: the task finishes (with a stored exception) on
        # its very first scheduling turn, well before shutdown begins.
        raise RuntimeError("boom-warm")

    gate_worker = threading.Event()

    async def _worker_stub(db: object) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.to_thread(gate_worker.wait, 5.0)
            raise

    db, app = _boot_app(tmp_path, monkeypatch, name="o20b.db")
    monkeypatch.setattr("wxverify.api.app.run_worker", _worker_stub)
    monkeypatch.setattr("wxverify.api.app.warm_read_cache", _warm_stub)
    client = TestClient(app)
    try:
        with caplog.at_level(logging.ERROR, logger="wxverify.api.app"):
            client.__enter__()
            resp = client.get("/api/health/monitor")
            assert resp.status_code == 200
            # `warm`'s task has had the whole startup-plus-one-round-trip
            # window to run to completion (it never awaits anything).

            shutdown = threading.Thread(target=client.__exit__, args=(None, None, None))
            shutdown.start()
            try:
                # Shutdown must still be blocked at `await worker` -- the
                # warm-failure log must not have appeared yet.
                shutdown.join(timeout=0.3)
                assert shutdown.is_alive()
                assert not any(
                    "read-cache warm failed" in r.getMessage() for r in caplog.records
                )
            finally:
                gate_worker.set()
            shutdown.join(timeout=5.0)
            assert not shutdown.is_alive()

        assert any("read-cache warm failed" in r.getMessage() for r in caplog.records)
    finally:
        gate_worker.set()
        close_db()


# ---------------------------------------------------------------------------
# O21 - reset_read_cache isolates two databases, negative control included
# ---------------------------------------------------------------------------


def test_o21_reset_read_cache_isolates_two_databases() -> None:
    conn_a = asof_conn()
    site_a = _site(conn_a)
    run_id = _full_run(conn_a, site_a, state="published", dominant_key=1)
    conn_a.commit()
    value_a = rc.cached_daily_rank_conclusions(conn_a, run_id)
    assert value_a["wind"]["selected_entity_key"] == "1"

    rc.reset_read_cache()

    conn_b = asof_conn()
    site_b = _site(conn_b)
    same_run_id = _new_run(conn_b, site_b, state="running")
    assert same_run_id == run_id  # same nominal id, materially different evidence
    _seed_wind(conn_b, same_run_id, dominant_key=9, other_key=10)
    _seed_precip(conn_b, same_run_id)
    _seed_verdict(conn_b, same_run_id)
    publish_run(conn_b, site_b, same_run_id)
    conn_b.commit()

    value_b = rc.cached_daily_rank_conclusions(conn_b, run_id)
    assert value_b["wind"]["selected_entity_key"] == "9"
    assert value_b != value_a


def test_o21_negative_control_without_reset_serves_the_stale_database() -> None:
    conn_a = asof_conn()
    site_a = _site(conn_a)
    run_id = _full_run(conn_a, site_a, state="published", dominant_key=1)
    conn_a.commit()
    value_a = rc.cached_daily_rank_conclusions(conn_a, run_id)
    assert value_a["wind"]["selected_entity_key"] == "1"

    # No reset_read_cache() here -- the negative control.
    conn_b = asof_conn()
    site_b = _site(conn_b)
    same_run_id = _new_run(conn_b, site_b, state="running")
    assert same_run_id == run_id
    _seed_wind(conn_b, same_run_id, dominant_key=9, other_key=10)
    _seed_precip(conn_b, same_run_id)
    _seed_verdict(conn_b, same_run_id)
    publish_run(conn_b, site_b, same_run_id)
    conn_b.commit()

    value_b = rc.cached_daily_rank_conclusions(conn_b, run_id)
    assert value_b == value_a  # stale: served from A's entry, not re-derived
    assert value_b["wind"]["selected_entity_key"] == "1"


# ---------------------------------------------------------------------------
# O22 - An earlier-resolved warm cannot overwrite a later one's pins
# (kills M12)
# ---------------------------------------------------------------------------


def test_o22_earlier_resolved_warm_cannot_overwrite_a_later_warms_pins(
    tmp_path: Path,
) -> None:
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o22.db"))
    conn = db._conn  # noqa: SLF001
    try:
        site_id = _site(conn)
        run_a = _full_run(conn, site_id, state="published", dominant_key=1)
        conn.commit()

        entered_a = threading.Event()
        gate_a = threading.Event()
        real_w7 = rc.daily_rank_conclusions

        def _blocking_w7(
            conn: sqlite3.Connection,
            run_id: int,
            *,
            leads: tuple[int, ...] | None = None,
        ) -> dict[str, dict[str, object]]:
            if run_id == run_a:
                entered_a.set()
                assert gate_a.wait(timeout=5.0)
            return real_w7(conn, run_id, leads=leads)

        import wxverify.verification.read_cache as rc_mod

        rc_mod.daily_rank_conclusions = _blocking_w7  # type: ignore[assignment]
        try:

            async def _scenario() -> None:
                task_a = asyncio.create_task(rc.warm_read_cache(db))
                await asyncio.wait_for(
                    asyncio.to_thread(entered_a.wait, 5.0), timeout=5.0
                )

                run_b = _full_run(
                    conn, site_id, state="running", dominant_key=2, other_key=3
                )
                publish_run(conn, site_id, run_b)
                conn.commit()

                await rc.warm_read_cache(db)  # warm B, runs to completion
                gen = rc.current_db_generation()
                b_keys = {(gen, rc._W7_NAME, run_b), (gen, rc._W13_NAME, run_b)}
                assert set(rc._PINNED.keys()) == b_keys

                try:
                    gate_a.set()
                    await asyncio.wait({task_a}, timeout=5.0)
                finally:
                    gate_a.set()
                assert task_a.done()
                assert task_a.exception() is None

                assert set(rc._PINNED.keys()) == b_keys
                assert not any(k[2] == run_a for k in rc._PINNED)

            asyncio.run(_scenario())
        finally:
            rc_mod.daily_rank_conclusions = real_w7  # type: ignore[assignment]
    finally:
        close_db()


# ---------------------------------------------------------------------------
# O23 - A generation change during a derivation stores nothing (kills M13)
# ---------------------------------------------------------------------------


def test_o23_generation_change_mid_derivation_stores_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = asof_conn()
    site_id = _site(conn)
    run_id = _full_run(conn, site_id, state="published")
    conn.commit()

    gens = iter([5] + [6] * 100)
    monkeypatch.setattr(rc, "current_db_generation", lambda: next(gens))

    value = rc.cached_daily_rank_conclusions(conn, run_id)
    assert value["wind"]["value"] in {
        "indicated",
        "not_indicated",
        "indicated_all_depths",
        "indicated_on_subset",
        "not_assessable",
    }
    assert (5, rc._W7_NAME, run_id) not in rc._ENTRIES
    assert (5, rc._W7_NAME, run_id) not in rc._PINNED
    assert (6, rc._W7_NAME, run_id) not in rc._ENTRIES
    assert (6, rc._W7_NAME, run_id) not in rc._PINNED

    calls = {"n": 0}
    real = rc.daily_rank_conclusions

    def _counting(
        conn: sqlite3.Connection, run_id: int, *, leads: tuple[int, ...] | None = None
    ) -> dict[str, dict[str, object]]:
        calls["n"] += 1
        return real(conn, run_id, leads=leads)

    monkeypatch.setattr(rc, "daily_rank_conclusions", _counting)
    monkeypatch.setattr(rc, "current_db_generation", lambda: 6)
    rc.cached_daily_rank_conclusions(conn, run_id)
    assert calls["n"] == 1  # re-derives; nothing was cached before


# ---------------------------------------------------------------------------
# O24 - The ticket cannot be split from the snapshot (kills M16)
# ---------------------------------------------------------------------------


def test_o24_ticket_order_matches_snapshot_order(tmp_path: Path) -> None:
    """The k-th arbitration ticket must see exactly k completed snapshots.

    O24 probes the ORDERING relation between a published-pointer snapshot
    and the epoch ticket that orders it, not the ``_WARM_ARBITRATION`` lock
    itself. A probe planted on the ticket call alone cannot distinguish the
    correct code from a faithful split (ticket moved into its own
    ``db.read``, still under the same lock, merely no longer indivisible
    with the snapshot): while one warm blocks inside the ticket call it is
    *holding* the lock, which excludes a second warm's ticket in both the
    correct code and the split alike -- the counter observes the lock, not
    the indivisibility. This oracle instead pauses at the real ``await``
    boundary AFTER a snapshot's ``db.read`` has returned (the lock already
    released), which is exactly the window a split opens and the combined
    call does not.

    ``snapshots["n"]`` increments after ``_published_targets`` returns;
    ``tickets`` records the value of ``snapshots["n"]`` observed at the
    moment each ticket is taken. Under indivisibility the k-th ticket must
    see exactly k completed snapshots, so the correct code (one combined
    ``db.read`` per warm) yields ``tickets == [1, 2]``. The split survivor
    lets a second warm's snapshot-and-ticket complete inside the window
    opened by the first warm's now-separate, now-unlocked pause, so the
    first warm's own ticket call -- taken only after it resumes -- observes
    BOTH snapshots already done and pins its own (by-then-stale) targets
    over the second warm's fresher ones: ``tickets == [2, 2]``.
    """
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o24.db"))
    conn = db._conn  # noqa: SLF001
    try:
        site_id = _site(conn)
        _full_run(conn, site_id, state="published", dominant_key=1)
        conn.commit()

        real_next_epoch = rc._next_warm_epoch  # noqa: SLF001
        real_published_targets = rc._published_targets  # noqa: SLF001
        real_read = db.read

        snapshots = {"n": 0}
        tickets: list[int] = []
        first_read_returned = threading.Event()
        release_first_read = threading.Event()
        first_claimed = {"done": False}

        def _counted_targets(conn: sqlite3.Connection) -> list[tuple[int, int]]:
            result = real_published_targets(conn)
            snapshots["n"] += 1
            return result

        def _counted_next_epoch() -> int:
            tickets.append(snapshots["n"])
            return real_next_epoch()

        async def _read_wrapper(
            fn: Callable[[sqlite3.Connection], _ReadT],
        ) -> _ReadT:
            result = await real_read(fn)
            if not first_claimed["done"]:
                first_claimed["done"] = True
                first_read_returned.set()
                # Rendezvous on the real await boundary: pause AFTER the
                # snapshot's db.read has fully returned (lock released),
                # never a sleep -- the second warm is what determines how
                # long this pause needs to last, and it signals directly.
                assert await asyncio.to_thread(release_first_read.wait, 5.0)
            return result

        import wxverify.verification.read_cache as rc_mod

        rc_mod._published_targets = _counted_targets  # type: ignore[assignment] # noqa: SLF001
        rc_mod._next_warm_epoch = _counted_next_epoch  # type: ignore[assignment] # noqa: SLF001
        db.read = _read_wrapper  # type: ignore[method-assign]
        try:

            async def _scenario() -> None:
                task_a = asyncio.create_task(rc.warm_read_cache(db))
                task_b: asyncio.Task[None] | None = None
                try:
                    assert await asyncio.wait_for(
                        asyncio.to_thread(first_read_returned.wait, 5.0),
                        timeout=5.0,
                    )

                    run_b = _full_run(
                        conn, site_id, state="running", dominant_key=2, other_key=3
                    )
                    publish_run(conn, site_id, run_b)
                    conn.commit()
                    task_b = asyncio.create_task(rc.warm_read_cache(db))
                    # B must actually finish before A is released, so the
                    # ticket order below is never a race against how far B
                    # got -- it observably reached completion, not "waited
                    # and nothing happened yet".
                    await asyncio.wait_for(task_b, timeout=5.0)

                    release_first_read.set()
                    await asyncio.wait_for(task_a, timeout=5.0)

                    assert task_a.exception() is None
                    assert task_b.exception() is None
                    assert tickets == [1, 2]

                    gen = rc.current_db_generation()
                    b_keys = {
                        (gen, rc._W7_NAME, run_b),
                        (gen, rc._W13_NAME, run_b),
                    }
                    assert set(rc._PINNED.keys()) == b_keys
                finally:
                    # Bounded cleanup: an assertion failure anywhere above
                    # must never leave the blocked-thread rendezvous or
                    # either task alive past this test.
                    release_first_read.set()
                    for task in (task_a, task_b):
                        if task is not None and not task.done():
                            task.cancel()
                    for task in (task_a, task_b):
                        if task is not None:
                            with contextlib.suppress(BaseException):
                                await task

            asyncio.run(_scenario())
        finally:
            rc_mod._published_targets = real_published_targets  # type: ignore[assignment] # noqa: SLF001
            rc_mod._next_warm_epoch = real_next_epoch  # type: ignore[assignment] # noqa: SLF001
            db.read = real_read  # type: ignore[method-assign]
    finally:
        close_db()


# ---------------------------------------------------------------------------
# O25 - The pointer snapshot is one statement (kills M15)
# ---------------------------------------------------------------------------


def test_o25_pointer_snapshot_is_one_statement_and_agrees_with_published_run_key() -> (
    None
):
    from wxverify.verification.runs import published_run_id

    conn = asof_conn()
    site_with_pointer_1 = _site(conn, name="site-alpha")
    run_1 = _full_run(conn, site_with_pointer_1, state="published")
    site_with_pointer_2 = _site(conn, name="site-beta")
    run_2 = _full_run(conn, site_with_pointer_2, state="published", dominant_key=7)
    site_no_pointer = _site(conn, name="site-gamma")
    _full_run(conn, site_no_pointer, state="running")
    site_disabled_with_pointer = _site(conn, name="site-delta", enabled=False)
    run_disabled = _full_run(conn, site_disabled_with_pointer, state="published")
    conn.commit()

    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        targets = rc._published_targets(conn)  # noqa: SLF001
    finally:
        conn.set_trace_callback(None)

    assert len(statements) == 1
    assert "sites" in statements[0]
    assert "runtime_state" in statements[0]

    expected = sorted([site_with_pointer_1, site_with_pointer_2])
    assert [site_id for site_id, _run_id in targets] == expected
    assert not any(site_id == site_disabled_with_pointer for site_id, _r in targets)

    for site_id, run_id in targets:
        assert run_id == published_run_id(conn, site_id)
    assert run_disabled  # referenced: seeded but never expected in targets
    assert {run_1, run_2} == {run for _s, run in targets}


# ---------------------------------------------------------------------------
# O26 - reset_read_cache() is a barrier, not a wipe (kills M17, M18)
# ---------------------------------------------------------------------------


def test_o26_reset_read_cache_is_a_barrier_not_a_wipe(tmp_path: Path) -> None:
    from wxverify.db.connection import close_db, init_db

    close_db()
    db = init_db(str(tmp_path / "o26.db"))
    conn = db._conn  # noqa: SLF001
    try:
        site_1 = _site(conn, name="site-alpha")
        run_1 = _full_run(conn, site_1, state="published", dominant_key=1)
        site_2 = _site(conn, name="site-beta")
        run_2 = _full_run(conn, site_2, state="published", dominant_key=2, other_key=3)
        conn.commit()

        entered = threading.Event()
        gate = threading.Event()
        real_w7 = rc.daily_rank_conclusions
        first_seen: dict[str, int] = {}

        def _blocking_first(
            conn: sqlite3.Connection,
            run_id: int,
            *,
            leads: tuple[int, ...] | None = None,
        ) -> dict[str, dict[str, object]]:
            if "run" not in first_seen:
                first_seen["run"] = run_id
                entered.set()
                assert gate.wait(timeout=5.0)
            return real_w7(conn, run_id, leads=leads)

        import wxverify.verification.read_cache as rc_mod

        rc_mod.daily_rank_conclusions = _blocking_first  # type: ignore[assignment]
        try:
            epoch_before = rc._warm_epoch  # noqa: SLF001

            async def _scenario() -> None:
                task = asyncio.create_task(rc.warm_read_cache(db))
                await asyncio.wait_for(
                    asyncio.to_thread(entered.wait, 5.0), timeout=5.0
                )
                rc.reset_read_cache()
                gate.set()
                await asyncio.wait({task}, timeout=5.0)
                assert task.done()
                assert task.exception() is None

            asyncio.run(_scenario())
        finally:
            rc_mod.daily_rank_conclusions = real_w7  # type: ignore[assignment]

        assert rc._PINNED == {}
        assert rc._ENTRIES == {}
        assert rc._warm_epoch > epoch_before  # noqa: SLF001
        assert run_1 or run_2  # referenced: seeded so first_seen names a real run
    finally:
        close_db()
