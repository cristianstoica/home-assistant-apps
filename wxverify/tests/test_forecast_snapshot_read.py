"""Oracle suite for the read_snapshot rebase of the Forecast page / tiles
auto-poll / hourly-detail / ranking-dispatch read paths (plan
2026-09-26-forecast-snapshot-read).

Mirrors tests/test_freshness_read_snapshot.py's and
tests/test_leaderboard_read_snapshot.py's harness shape: real tmp-file
SQLite DBs (never :memory:, since the app's WAL reader connections are
separate from whatever writes the fixture rows), a side sqlite3 connection
for injected concurrent writes, and TestClient for HTTP-level behavior.
Every monkeypatch seam names the module that actually CALLS the target
(Python binds names at call time into the calling module's namespace), not
the module that defines it.

Synthetic fixtures only (public repo): fake site names/coordinates, no real
station id, key, or coordinate anywhere in this file, including comments.
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import assert_read_pool_at_rest
from tests.test_forecast_tiles_poll import (
    _current_fingerprint,
    _feed_id,
    _high_low_values,
    _init_tmp_db,
    _insert_sample,
    _make_site,
)
from tests.test_forecast_tiles_poll import _make_app as _make_app_base
from tests.test_leaderboard_cache_backed import _init_tmp_db as _init_lb_db
from tests.test_leaderboard_read_snapshot import _hit_fixture, _side_conn
from wxverify import config
from wxverify.core.timeutil import floor_hour, isoformat_utc, utc_now
from wxverify.db.connection import _READ_POOL_SIZE, Database, get_db
from wxverify.db.snapshot import read_snapshot
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast.data import forecast_ranking_with_status
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.leaderboard import resolve_window
from wxverify.scoring.metrics import strategy_for
from wxverify.settings.keys import get_number_setting, set_setting
from wxverify.web.routes import _load_forecast_context

# ---------------------------------------------------------------------------
# Harness (reuse list per plan §11.1; only new, local helpers below).
# ---------------------------------------------------------------------------


async def _no_warm(db: object) -> None:
    return None


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.warm_read_cache", _no_warm)
    return _make_app_base(monkeypatch)


@dataclass
class Seed:
    site_id: int
    feed_a: int
    feed_b: int
    now: datetime
    s0_issued_at: str


def _seed_s0(conn: sqlite3.Connection) -> Seed:
    """A copy of the S0 seed at test_forecast_tiles_poll.py:261-339, with
    site name "Synthetic Site A"."""
    site_id = _make_site(conn, "Synthetic Site A")
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "min_n", "1")
    set_setting(conn, "forecast_blend_depth", "2")

    now = utc_now()
    tomorrow = now.date() + timedelta(days=1)
    issued_at = isoformat_utc(floor_hour(now))
    for feed_id, value in ((feed_a, 11.0), (feed_b, 15.0)):
        for hour in range(24):
            _insert_sample(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable="temperature",
                issued_at=issued_at,
                valid_at=f"{tomorrow.isoformat()}T{hour:02d}:00:00Z",
                lead_hours=hour + 1,
                value=value,
            )

    far_valid_ats = [
        "2035-06-30T00:00:00Z",
        "2035-06-30T01:00:00Z",
        "2035-06-30T02:00:00Z",
    ]
    far_lead_hours = [1, 2, 3]
    for target_feed, forecast in (
        (persistence_id, 8.0),
        (feed_a, 10.5),
        (feed_b, 9.0),
    ):
        for valid_at, lead_hours in zip(far_valid_ats, far_lead_hours, strict=True):
            error = forecast - 10.0
            conn.execute(
                """
                INSERT INTO forecast_pairs
                    (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                     day_ahead, forecast, observed, error, abs_error, sq_error,
                     tz_generation_id)
                VALUES (?, ?, 'temperature', '2035-06-29T00:00:00Z', ?, ?, 1, ?,
                        10.0, ?, ?, ?, ?)
                """,
                (
                    site_id,
                    target_feed,
                    valid_at,
                    lead_hours,
                    forecast,
                    error,
                    abs(error),
                    error * error,
                    ensure_published_generation(conn, site_id),
                ),
            )

    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    resolved = resolve_window(conn, "rolling")
    for target_feed in (feed_a, feed_b, persistence_id):
        result = strategy_for("temperature").aggregate(
            conn,
            site_id=site_id,
            feed_id=target_feed,
            variable="temperature",
            day_ahead=1,
            window_cutoff=resolved.cutoff,
            min_n=min_n,
        )
        upsert_score_cache(
            conn,
            site_id=site_id,
            feed_id=target_feed,
            variable="temperature",
            day_ahead=1,
            window_key=resolved.window_key,
            result=result,
            computed_at=isoformat_utc(),
        )

    return Seed(
        site_id=site_id, feed_a=feed_a, feed_b=feed_b, now=now, s0_issued_at=issued_at
    )


def _write_r2(conn: sqlite3.Connection, seed: Seed) -> None:
    """48 rows: feed_a=21.0/feed_b=25.0 on the same valid_ats as S0, issued
    strictly after S0 (still whole-second, same UTC date)."""
    tomorrow = seed.now.date() + timedelta(days=1)
    issued_at = isoformat_utc(floor_hour(seed.now) + timedelta(minutes=30))
    for feed_id, value in ((seed.feed_a, 21.0), (seed.feed_b, 25.0)):
        for hour in range(24):
            _insert_sample(
                conn,
                site_id=seed.site_id,
                feed_id=feed_id,
                variable="temperature",
                issued_at=issued_at,
                valid_at=f"{tomorrow.isoformat()}T{hour:02d}:00:00Z",
                lead_hours=hour + 1,
                value=value,
            )


def _commit_once(
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    write: Callable[[sqlite3.Connection], None] | None,
    *,
    when: str,
) -> dict[str, int]:
    """Wrap the dotted binding named by ``target`` (in the CONSUMING module)
    so that, on the first call reached only, and only if ``write`` is not
    None, a side connection commits ``write`` before or after the original
    call runs (``when``). Returns a live counts dict: "reached" counts every
    call through the wrapper, "fired" counts the injected commit."""
    module_path, _, attr = target.rpartition(".")
    module = __import__(module_path, fromlist=[attr])
    original = getattr(module, attr)
    counts = {"reached": 0, "fired": 0}

    def _side_commit(fn: Callable[[sqlite3.Connection], None]) -> None:
        side = _side_conn(config.db_path)
        try:
            side.execute("BEGIN IMMEDIATE")
            fn(side)
            side.commit()
        finally:
            side.close()

    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        counts["reached"] += 1
        if write is not None and counts["reached"] == 1:
            if when == "before":
                _side_commit(write)
                result = original(*args, **kwargs)
            else:
                result = original(*args, **kwargs)
                _side_commit(write)
            counts["fired"] += 1
            return result
        return original(*args, **kwargs)

    monkeypatch.setattr(target, _wrapped)
    return counts


_TOKEN_RE = re.compile(r'fingerprint=([0-9-]*)"')
_DEPTH_LINE_RE = re.compile(r'<p class="muted depth-line">(.*?)</p>', re.DOTALL)


def _token(html: str) -> str:
    matches = _TOKEN_RE.findall(html)
    assert len(matches) == 1, f"expected exactly one fingerprint token: {matches!r}"
    return matches[0]


def _depth_line(html: str) -> str:
    match = _DEPTH_LINE_RE.search(html)
    assert match is not None, "no depth-line paragraph found"
    return " ".join(match.group(1).split())


def _heading(name: str) -> str:
    return f"<p>{name} - combined forecast from the best-performing feeds</p>"


def _snapshot_records(
    records: Iterable[logging.LogRecord],
) -> list[tuple[str, float]]:
    return [
        (r.args[0], r.args[1])  # type: ignore[index]
        for r in records
        if r.name == "wxverify.db.snapshot" and r.msg == "db snapshot %s held=%.1fms"
    ]


def _assert_no_settle_warning(records: Iterable[logging.LogRecord]) -> None:
    assert not any(
        r.name == "wxverify.db.connection"
        and r.getMessage() == "reader returned mid-transaction; rolling back"
        for r in records
    )


def _no_reader_in_transaction(db: Database) -> bool:
    def _in_txn(conn: sqlite3.Connection) -> bool:
        try:
            return bool(conn.in_transaction)
        except sqlite3.ProgrammingError:
            return False

    return not any(_in_txn(c) for c in db._read_conns)  # noqa: SLF001


def _assert_clean_after_request(
    db: Database, caplog: pytest.LogCaptureFixture, *, label: str
) -> list[tuple[str, float]]:
    snap = _snapshot_records(caplog.records)
    labels = [entry[0] for entry in snap]
    assert labels == [label], f"expected [{label!r}], got {labels!r}"
    assert_read_pool_at_rest(db)
    assert _no_reader_in_transaction(db)
    _assert_no_settle_warning(caplog.records)
    return snap


def _assert_checkpoint_clean(db_path: str) -> None:
    """Runs a PASSIVE checkpoint on an independent connection: proves the
    snapshot has actually ENDED (the DEBUG "held=" line is emitted before
    conn.rollback(), snapshot.py:63-66, so it only identifies labels)."""
    side = _side_conn(db_path)
    try:
        side.execute("BEGIN IMMEDIATE")
        side.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES "
            "('snapshot_probe', '1')"
        )
        side.commit()
        busy, log, ckpt = 1, 0, 0
        for _ in range(3):
            busy, log, ckpt = side.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            if busy == 0:
                break
            import time

            time.sleep(0.05)
        if busy != 0 or log <= 0:
            pytest.fail("checkpoint busy or empty WAL: cannot judge")
        assert ckpt == log, f"a reader still holds a snapshot: {ckpt} < {log}"
    finally:
        try:
            if side.in_transaction:
                side.rollback()
            side.execute("BEGIN IMMEDIATE")
            side.execute("DELETE FROM settings WHERE key = 'snapshot_probe'")
            side.commit()
        finally:
            side.close()


# ---------------------------------------------------------------------------
# Fixture constants (§11.2): predicted, then confirmed by the controls test.
# ---------------------------------------------------------------------------

V0 = ["13° / 13°"]
V1 = ["23° / 23°"]
V2 = ["21° / 21°"]
T0 = "48"
T1 = "96"
D2 = "Blend depth: temperature 2 (global) · wind 2 (global) · precip 2 (global)"
D1 = "Blend depth: temperature 1 (global) · wind 1 (global) · precip 1 (global)"


# ---------------------------------------------------------------------------
# Controls: stages 0-4, no injected concurrency. Also confirms §11.2.
# ---------------------------------------------------------------------------


def test_controls_stage_sequence_confirms_fixture_constants(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    seed = _seed_s0(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        # Stage 0.
        page0 = client.get(f"/forecast?site={seed.site_id}")
        assert page0.status_code == 200
        assert _high_low_values(page0.text) == V0
        assert _token(page0.text) == T0
        assert _depth_line(page0.text) == D2
        assert _heading("Synthetic Site A") in page0.text

        # Stage 1.
        poll1 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        assert poll1.status_code == 204

        # Stage 2.
        hourly0 = client.get(f"/api/forecast/hourly?site={seed.site_id}&day=1")
        assert hourly0.status_code == 200
        payload0 = hourly0.json()
        assert payload0["blend"]["temp_c"] == [13.0] * 24
        assert {f["feed_id"] for f in payload0["feeds"]} == {seed.feed_a, seed.feed_b}

        # Stage 2b.
        get_db().write_sync(lambda c: set_setting(c, "forecast_blend_depth", "1"))
        hourly_d1 = client.get(f"/api/forecast/hourly?site={seed.site_id}&day=1")
        payload_d1 = hourly_d1.json()
        assert payload_d1["blend"]["temp_c"] == [11.0] * 24
        assert {f["feed_id"] for f in payload_d1["feeds"]} == {seed.feed_a}
        get_db().write_sync(lambda c: set_setting(c, "forecast_blend_depth", "2"))

        # Stage 3.
        get_db().write_sync(lambda c: _write_r2(c, seed))
        page3 = client.get(f"/forecast?site={seed.site_id}")
        assert page3.status_code == 200
        assert _high_low_values(page3.text) == V1
        assert _token(page3.text) == T1

        poll_t0 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        assert poll_t0.status_code == 200
        assert _high_low_values(poll_t0.text) == V1
        assert _token(poll_t0.text) == T1

        poll_t1 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T1}")
        assert poll_t1.status_code == 204

        # Stage 4.
        get_db().write_sync(lambda c: set_setting(c, "forecast_blend_depth", "1"))
        page4 = client.get(f"/forecast?site={seed.site_id}")
        assert page4.status_code == 200
        assert _high_low_values(page4.text) == V2
        assert _depth_line(page4.text) == D1

        hourly4 = client.get(f"/api/forecast/hourly?site={seed.site_id}&day=1")
        payload4 = hourly4.json()
        assert payload4["blend"]["temp_c"] == [21.0] * 24
        assert {f["feed_id"] for f in payload4["feeds"]} == {seed.feed_a}


# ---------------------------------------------------------------------------
# T-A .. T-F: every commit case parametrized write in {commit, control}.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("write_mode", ["commit", "control"], ids=["commit", "control"])
def test_ta_page_commit_between_samples_and_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, write_mode: str
) -> None:
    conn = _init_tmp_db(tmp_path)
    seed = _seed_s0(conn)
    app = _make_app(monkeypatch)
    write = (lambda c: _write_r2(c, seed)) if write_mode == "commit" else None
    counts = _commit_once(
        monkeypatch,
        "wxverify.forecast.service.forecast_fingerprint",
        write,
        when="before",
    )
    with TestClient(app) as client:
        page = client.get(f"/forecast?site={seed.site_id}")
        assert page.status_code == 200
        assert _high_low_values(page.text) == V0
        assert _token(page.text) == T0

        poll_t0 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        if write_mode == "commit":
            assert poll_t0.status_code == 200
            assert _high_low_values(poll_t0.text) == V1
            assert _token(poll_t0.text) == T1

            poll_t1 = client.get(
                f"/forecast/tiles?site={seed.site_id}&fingerprint={T1}"
            )
            assert poll_t1.status_code == 204
        else:
            assert poll_t0.status_code == 204

    assert counts["reached"] >= 1, "wrapper not reached"
    assert counts["fired"] == (1 if write_mode == "commit" else 0)


@pytest.mark.parametrize("write_mode", ["commit", "control"], ids=["commit", "control"])
def test_tb_poll_empty_token_first_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, write_mode: str
) -> None:
    conn = _init_tmp_db(tmp_path)
    seed = _seed_s0(conn)
    app = _make_app(monkeypatch)
    write = (lambda c: _write_r2(c, seed)) if write_mode == "commit" else None
    counts = _commit_once(
        monkeypatch,
        "wxverify.forecast.service.forecast_fingerprint",
        write,
        when="before",
    )
    with TestClient(app) as client:
        r1 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint=")
        assert r1.status_code == 200
        assert _high_low_values(r1.text) == V0
        assert _token(r1.text) == T0

        r2 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        if write_mode == "commit":
            assert r2.status_code == 200
            assert _high_low_values(r2.text) == V1
            assert _token(r2.text) == T1

            r3 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T1}")
            assert r3.status_code == 204
        else:
            assert r2.status_code == 204

    assert counts["reached"] >= 1, "wrapper not reached"
    assert counts["fired"] == (1 if write_mode == "commit" else 0)


@pytest.mark.parametrize("write_mode", ["commit", "control"], ids=["commit", "control"])
def test_tc_two_commits_depth_and_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, write_mode: str
) -> None:
    conn = _init_tmp_db(tmp_path)
    seed = _seed_s0(conn)
    app = _make_app(monkeypatch)
    write_r2 = (lambda c: _write_r2(c, seed)) if write_mode == "commit" else None
    write_depth1 = (
        (lambda c: set_setting(c, "forecast_blend_depth", "1"))
        if write_mode == "commit"
        else None
    )
    counts_fp = _commit_once(
        monkeypatch,
        "wxverify.forecast.service.forecast_fingerprint",
        write_r2,
        when="before",
    )
    counts_depth = _commit_once(
        monkeypatch,
        "wxverify.web.routes.effective_blend_depths",
        write_depth1,
        when="before",
    )
    with TestClient(app) as client:
        page = client.get(f"/forecast?site={seed.site_id}")
        assert page.status_code == 200
        assert _high_low_values(page.text) == V0
        assert _token(page.text) == T0
        assert _depth_line(page.text) == D2

        poll_t0 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        if write_mode == "commit":
            assert poll_t0.status_code == 200
            assert _high_low_values(poll_t0.text) == V2
            assert _token(poll_t0.text) == T1

            poll_t1 = client.get(
                f"/forecast/tiles?site={seed.site_id}&fingerprint={T1}"
            )
            assert poll_t1.status_code == 204

            fresh_page = client.get(f"/forecast?site={seed.site_id}")
            assert fresh_page.status_code == 200
            assert _depth_line(fresh_page.text) == D1
        else:
            assert poll_t0.status_code == 204

    assert counts_fp["reached"] >= 1, "wrapper not reached (fingerprint)"
    assert counts_fp["fired"] == (1 if write_mode == "commit" else 0)
    assert counts_depth["reached"] >= 1, "wrapper not reached (depth)"
    assert counts_depth["fired"] == (1 if write_mode == "commit" else 0)


@pytest.mark.parametrize("write_mode", ["commit", "control"], ids=["commit", "control"])
def test_td_commit_before_build_mixes_heading_and_tiles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, write_mode: str
) -> None:
    conn = _init_tmp_db(tmp_path)
    seed = _seed_s0(conn)
    app = _make_app(monkeypatch)

    def _write(c: sqlite3.Connection) -> None:
        c.execute(
            "UPDATE sites SET name = ? WHERE id = ?", ("Synthetic Site B", seed.site_id)
        )
        _write_r2(c, seed)

    write = _write if write_mode == "commit" else None
    counts = _commit_once(
        monkeypatch, "wxverify.web.routes.build_forecast", write, when="before"
    )
    with TestClient(app) as client:
        page = client.get(f"/forecast?site={seed.site_id}")
        assert page.status_code == 200
        assert _heading("Synthetic Site A") in page.text
        assert _high_low_values(page.text) == V0
        assert _token(page.text) == T0

        poll_t0 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        if write_mode == "commit":
            assert poll_t0.status_code == 200
            assert _high_low_values(poll_t0.text) == V1
            assert _token(poll_t0.text) == T1

            fresh_page = client.get(f"/forecast?site={seed.site_id}")
            assert fresh_page.status_code == 200
            assert _heading("Synthetic Site B") in fresh_page.text
            assert _high_low_values(fresh_page.text) == V1
            assert _token(fresh_page.text) == T1
        else:
            assert poll_t0.status_code == 204

            fresh_page = client.get(f"/forecast?site={seed.site_id}")
            assert fresh_page.status_code == 200
            assert _heading("Synthetic Site A") in fresh_page.text

    assert counts["reached"] >= 1, "wrapper not reached"
    assert counts["fired"] == (1 if write_mode == "commit" else 0)


@pytest.mark.parametrize("write_mode", ["commit", "control"], ids=["commit", "control"])
def test_te1_commit_right_after_early_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, write_mode: str
) -> None:
    conn = _init_tmp_db(tmp_path)
    seed = _seed_s0(conn)
    app = _make_app(monkeypatch)
    write = (lambda c: _write_r2(c, seed)) if write_mode == "commit" else None
    counts = _commit_once(
        monkeypatch, "wxverify.web.routes.forecast_fingerprint", write, when="after"
    )
    with TestClient(app) as client:
        r1 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint=")
        assert r1.status_code == 200
        assert _high_low_values(r1.text) == V0
        assert _token(r1.text) == T0

        r2 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        if write_mode == "commit":
            assert r2.status_code == 200
            assert _high_low_values(r2.text) == V1
            assert _token(r2.text) == T1
        else:
            assert r2.status_code == 204

    assert counts["reached"] >= 1, "wrapper not reached"
    assert counts["fired"] == (1 if write_mode == "commit" else 0)


@pytest.mark.parametrize("write_mode", ["commit", "control"], ids=["commit", "control"])
def test_te2_commit_after_check_not_lost_when_token_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, write_mode: str
) -> None:
    """As T-E1, but polling with the already-matching token: pins "a commit
    after the check is not lost". Green by construction on both codes."""
    conn = _init_tmp_db(tmp_path)
    seed = _seed_s0(conn)
    app = _make_app(monkeypatch)
    write = (lambda c: _write_r2(c, seed)) if write_mode == "commit" else None
    counts = _commit_once(
        monkeypatch, "wxverify.web.routes.forecast_fingerprint", write, when="after"
    )
    with TestClient(app) as client:
        r1 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        assert r1.status_code == 204

        r2 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        if write_mode == "commit":
            assert r2.status_code == 200
            assert _high_low_values(r2.text) == V1
            assert _token(r2.text) == T1
        else:
            assert r2.status_code == 204

    assert counts["reached"] >= 1, "wrapper not reached"
    assert counts["fired"] == (1 if write_mode == "commit" else 0)


@pytest.mark.parametrize("write_mode", ["commit", "control"], ids=["commit", "control"])
def test_tf_hourly_commit_before_depth_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, write_mode: str
) -> None:
    conn = _init_tmp_db(tmp_path)
    seed = _seed_s0(conn)
    app = _make_app(monkeypatch)

    def _write(c: sqlite3.Connection) -> None:
        _write_r2(c, seed)
        set_setting(c, "forecast_blend_depth", "1")

    write = _write if write_mode == "commit" else None
    counts = _commit_once(
        monkeypatch,
        "wxverify.forecast.service.effective_blend_depths",
        write,
        when="before",
    )
    with TestClient(app) as client:
        r1 = client.get(f"/api/forecast/hourly?site={seed.site_id}&day=1")
        assert r1.status_code == 200
        payload1 = r1.json()
        assert payload1["blend"]["temp_c"] == [13.0] * 24
        assert {f["feed_id"] for f in payload1["feeds"]} == {seed.feed_a, seed.feed_b}

        r2 = client.get(f"/api/forecast/hourly?site={seed.site_id}&day=1")
        payload2 = r2.json()
        if write_mode == "commit":
            assert payload2["blend"]["temp_c"] == [21.0] * 24
            assert {f["feed_id"] for f in payload2["feeds"]} == {seed.feed_a}
        else:
            assert payload2["blend"]["temp_c"] == [13.0] * 24
            assert {f["feed_id"] for f in payload2["feeds"]} == {
                seed.feed_a,
                seed.feed_b,
            }

    assert counts["reached"] >= 1, "wrapper not reached"
    assert counts["fired"] == (1 if write_mode == "commit" else 0)


# ---------------------------------------------------------------------------
# T-G: lifecycle -- every request labels and tears down cleanly.
# ---------------------------------------------------------------------------


def test_tg_lifecycle_labels_and_teardown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _init_tmp_db(tmp_path)
    seed = _seed_s0(conn)
    app = _make_app(monkeypatch)

    caplog.set_level(logging.WARNING, logger="wxverify.db.connection")
    caplog.set_level(logging.DEBUG, logger="wxverify.db.snapshot")

    held: list[tuple[str, float]] = []

    with TestClient(app) as client:
        db = get_db()

        caplog.clear()
        r1 = client.get("/")
        assert r1.status_code == 200
        held.extend(_assert_clean_after_request(db, caplog, label="forecast_page"))

        caplog.clear()
        r2 = client.get(f"/forecast?site={seed.site_id}")
        assert r2.status_code == 200
        held.extend(_assert_clean_after_request(db, caplog, label="forecast_page"))

        token = _current_fingerprint(seed.site_id)

        caplog.clear()
        r3 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={token}")
        assert r3.status_code == 204
        held.extend(_assert_clean_after_request(db, caplog, label="forecast_tiles"))

        db.write_sync(lambda c: _write_r2(c, seed))

        caplog.clear()
        r4 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={token}")
        assert r4.status_code == 200
        held.extend(_assert_clean_after_request(db, caplog, label="forecast_tiles"))

        caplog.clear()
        r5 = client.get(f"/api/forecast/hourly?site={seed.site_id}&day=1")
        assert r5.status_code == 200
        held.extend(_assert_clean_after_request(db, caplog, label="forecast_hourly"))

        _assert_checkpoint_clean(db.path)

    print(f"T-G snapshot held= values (label, held_ms): {held}")


# ---------------------------------------------------------------------------
# T-H: an exception raised inside the snapshot still rolls back cleanly.
# ---------------------------------------------------------------------------


def test_th_exception_inside_snapshot_still_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _init_tmp_db(tmp_path)
    seed = _seed_s0(conn)
    app = _make_app(monkeypatch)

    def _boom(*_args: object, **_kwargs: object) -> Any:
        raise sqlite3.OperationalError("synthetic")

    monkeypatch.setattr("wxverify.forecast.service.load_feed_freshness", _boom)

    caplog.set_level(logging.WARNING, logger="wxverify.db.connection")
    caplog.set_level(logging.DEBUG, logger="wxverify.db.snapshot")

    with TestClient(app, raise_server_exceptions=False) as client:
        caplog.clear()
        response = client.get(f"/forecast?site={seed.site_id}")
        assert response.status_code == 500

        db = get_db()
        _assert_clean_after_request(db, caplog, label="forecast_page")
        _assert_checkpoint_clean(db.path)


# ---------------------------------------------------------------------------
# T-I: cancellation mid-snapshot rolls back cleanly (O4's shape).
# ---------------------------------------------------------------------------


def test_ti_cancellation_mid_snapshot_rolls_back_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_read_forecast_context is imported INSIDE this test body: a
    module-level import would make the whole file fail to collect on
    57456a0, and the red run could no longer show which cases pass there."""
    from wxverify.web import routes as routes_module
    from wxverify.web.routes import _read_forecast_context

    db = Database(str(tmp_path / "t_i.db"))
    try:
        entered = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        recorded: list[bool] = []
        original_depths = routes_module.effective_blend_depths

        def _blocking_depths(conn: sqlite3.Connection) -> dict[str, object]:
            recorded.append(conn.in_transaction)
            entered.set()
            assert release.wait(timeout=2.0)
            return original_depths(conn)

        monkeypatch.setattr(routes_module, "effective_blend_depths", _blocking_depths)

        def _cb(conn: sqlite3.Connection) -> dict[str, object]:
            result = _read_forecast_context(conn, None, label="forecast_page")
            finished.set()
            return result

        async def _drive() -> None:
            task = asyncio.create_task(db.read(_cb))
            assert await asyncio.to_thread(entered.wait, 2.0)
            task.cancel()
            task.cancel()
            asyncio.get_running_loop().call_later(0.05, release.set)
            with pytest.raises(asyncio.CancelledError):
                await task

        records: list[logging.LogRecord] = []
        handler = logging.Handler()
        handler.setLevel(logging.WARNING)
        handler.emit = records.append  # type: ignore[method-assign]
        target_logger = logging.getLogger("wxverify.db.connection")
        target_logger.addHandler(handler)
        try:
            asyncio.run(_drive())
        finally:
            target_logger.removeHandler(handler)

        release.set()
        assert finished.wait(2.0)
        assert recorded == [True]

        assert_read_pool_at_rest(db)  # (a)
        assert _no_reader_in_transaction(db)  # (b)

        side = _side_conn(db.path)
        try:
            side.execute("BEGIN IMMEDIATE")
            side.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('t_i_probe', '1')"
            )
            side.commit()
        finally:
            side.close()
        for _ in range(_READ_POOL_SIZE):  # (c)
            row = asyncio.run(
                db.read(
                    lambda c: c.execute(
                        "SELECT value FROM settings WHERE key = 't_i_probe'"
                    ).fetchone()
                )
            )
            assert row["value"] == "1"

        _assert_no_settle_warning(records)  # (d)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# T-J1/T-J2: ranking dispatch, autocommit vs. inside a caller's snapshot.
# ---------------------------------------------------------------------------


def test_tj1_ranking_autocommit_uses_facade(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _init_lb_db(tmp_path)
    site_id, feed1, _feed2 = _hit_fixture(conn)

    caplog.set_level(logging.DEBUG, logger="wxverify.db.snapshot")
    caplog.clear()
    ranking = forecast_ranking_with_status(
        conn, site_id=site_id, variable="temperature", day_ahead=1
    )
    labels = [entry[0] for entry in _snapshot_records(caplog.records)]
    assert labels == ["leaderboard"]
    assert ranking.status == "hit"
    assert set(ranking.rows) == {feed1}


def test_tj2_ranking_inside_snapshot_uses_in_transaction_arm(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _init_lb_db(tmp_path)
    site_id, feed1, _feed2 = _hit_fixture(conn)

    caplog.set_level(logging.DEBUG, logger="wxverify.db.snapshot")
    with read_snapshot(conn, label="outer"):
        caplog.clear()
        ranking = forecast_ranking_with_status(
            conn, site_id=site_id, variable="temperature", day_ahead=1
        )

    labels = [entry[0] for entry in _snapshot_records(caplog.records)]
    assert labels == ["outer"]
    assert ranking.status == "hit"
    assert set(ranking.rows) == {feed1}


# ---------------------------------------------------------------------------
# T-K: _load_forecast_context refuses outside a snapshot, works inside one.
# ---------------------------------------------------------------------------


def test_tk_guard_refuses_without_snapshot(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)

    with pytest.raises(
        RuntimeError, match="_load_forecast_context needs the caller's read snapshot"
    ):
        _load_forecast_context(conn, None)

    with read_snapshot(conn, label="t_k"):
        result = _load_forecast_context(conn, None)
    assert set(result) == {"sites", "site", "view", "depths"}
