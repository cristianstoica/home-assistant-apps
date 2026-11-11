"""Oracle suite for the read_snapshot rebase of the Forecast page / tiles
auto-poll / hourly-detail / ranking-dispatch read paths.

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
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import (
    assert_read_pool_at_rest,
    seed_active_publication,
    seed_publication_ranking,
)
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
from wxverify.db.connection import _READ_POOL_SIZE, Database, close_db, get_db, init_db
from wxverify.db.snapshot import read_only_snapshot, read_snapshot
from wxverify.forecast.data import forecast_ranking_with_status
from wxverify.scoring.metrics import MetricSums, metrics_from_sums
from wxverify.settings.keys import get_number_setting, set_setting
from wxverify.web.routes import _load_forecast_context

# ---------------------------------------------------------------------------
# Harness (only new, local helpers below).
# ---------------------------------------------------------------------------


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
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

    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    # leaderboard_with_status's `rolling` window now always reads the
    # active publication, never a live recompute, so the three feeds'
    # rankings land in one site publication instead of score_cache.
    pub_id = seed_active_publication(
        conn,
        site_id=site_id,
        cutoff=date(2035, 6, 30),
        published_at=datetime(2035, 6, 30, tzinfo=UTC),
        window_days=30,
        min_n=min_n,
    )
    # No pair table is read for scoring; the sums below are hand-derived
    # as if from
    # 3 identical far-future hours (forecast/observed=10.0, persistence
    # forecast=8.0), through the surviving pure `metrics_from_sums`,
    # skill-paired 1:1 against persistence's own (also constant) error.
    forecasts = {persistence_id: 8.0, feed_a: 10.5, feed_b: 9.0}
    persistence_sq = (forecasts[persistence_id] - 10.0) ** 2
    for target_feed in (feed_a, feed_b, persistence_id):
        error = forecasts[target_feed] - 10.0
        sums: MetricSums = {
            "n": 3,
            "err_n": 3,
            "err_sum": error * 3,
            "abs_sum": abs(error) * 3,
            "sq_sum": error * error * 3,
            "skill_n": 3,
            "skill_sq_sum": error * error * 3,
            "skill_ref_sq_sum": persistence_sq * 3,
        }
        result = metrics_from_sums(sums, variable="temperature", min_n=min_n)
        seed_publication_ranking(
            conn,
            publication_id=pub_id,
            feed_id=target_feed,
            variable="temperature",
            day_ahead=1,
            window_key="w:30",
            result=result,
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


#: The region's hx-get now reads "...&fingerprint=<token>&day=<n>" (no
#: longer a closing quote right after the token), so the token is bounded by
#: the following "&day=" rather than by '"'. Jinja autoescapes the attribute
#: text, so the rendered HTML carries "&amp;" rather than a bare "&".
_TOKEN_RE = re.compile(r"fingerprint=([0-9a-z-]*)&amp;day=")
_ACTIVE_PILL_RE = re.compile(r'<a class="pill active"[^>]*>([^<]*)</a>')


def _token(html: str) -> str:
    matches = _TOKEN_RE.findall(html)
    assert len(matches) == 1, f"expected exactly one fingerprint token: {matches!r}"
    return matches[0]


def _site_label(html: str) -> str:
    """The active site pill's name -- the page dropped the per-site subtitle
    ("<site> - combined forecast from the best-performing feeds") this used
    to read; the site-picker pill is the one place a specific site's name
    is still rendered, so it is the replacement site-identity check."""
    match = _ACTIVE_PILL_RE.search(html)
    assert match is not None, "no active site pill found"
    return match.group(1)


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
# Fixture constants: predicted, then confirmed by the controls test.
# ---------------------------------------------------------------------------

V0 = ["13° / 13°"]
V1 = ["23° / 23°"]
V2 = ["21° / 21°"]
#: ``-p1`` is the seed's single active publication, the first and only
#: row `_seed_s0` inserts into `publications` (with the daily close on,
#: `forecast_fingerprint` always ends in `-p<id>` of that publication).
T0 = "48-p1"
T1 = "96-p1"


# ---------------------------------------------------------------------------
# Controls: stages 0-4, no injected concurrency. Also confirms the fixture constants.
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
        assert _high_low_values(page0.text)[1:2] == V0
        assert _token(page0.text) == T0
        assert _site_label(page0.text) == "Synthetic Site A"

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
        assert _high_low_values(page3.text)[1:2] == V1
        assert _token(page3.text) == T1

        poll_t0 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        assert poll_t0.status_code == 200
        assert _high_low_values(poll_t0.text)[1:2] == V1
        assert _token(poll_t0.text) == T1

        poll_t1 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T1}")
        assert poll_t1.status_code == 204

        # Stage 4.
        get_db().write_sync(lambda c: set_setting(c, "forecast_blend_depth", "1"))
        page4 = client.get(f"/forecast?site={seed.site_id}")
        assert page4.status_code == 200
        assert _high_low_values(page4.text)[1:2] == V2

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
        assert _high_low_values(page.text)[1:2] == V0
        assert _token(page.text) == T0

        poll_t0 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        if write_mode == "commit":
            assert poll_t0.status_code == 200
            assert _high_low_values(poll_t0.text)[1:2] == V1
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
        assert _high_low_values(r1.text)[1:2] == V0
        assert _token(r1.text) == T0

        r2 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        if write_mode == "commit":
            assert r2.status_code == 200
            assert _high_low_values(r2.text)[1:2] == V1
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
        "wxverify.forecast.service.effective_blend_depths",
        write_depth1,
        when="before",
    )
    with TestClient(app) as client:
        page = client.get(f"/forecast?site={seed.site_id}")
        assert page.status_code == 200
        assert _high_low_values(page.text)[1:2] == V0
        assert _token(page.text) == T0

        poll_t0 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        if write_mode == "commit":
            assert poll_t0.status_code == 200
            assert _high_low_values(poll_t0.text)[1:2] == V2
            assert _token(poll_t0.text) == T1

            poll_t1 = client.get(
                f"/forecast/tiles?site={seed.site_id}&fingerprint={T1}"
            )
            assert poll_t1.status_code == 204

            # The depth-1 value (V2 above) already proves the blend-depth
            # write landed together with the fingerprint-moving write in
            # the SAME read; there is no separate depth indicator left on
            # the page to check a second time (the
            # depth-line paragraph is gone).
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
        monkeypatch, "wxverify.web.routes.build_forecast_page", write, when="before"
    )
    with TestClient(app) as client:
        page = client.get(f"/forecast?site={seed.site_id}")
        assert page.status_code == 200
        assert _site_label(page.text) == "Synthetic Site A"
        assert _high_low_values(page.text)[1:2] == V0
        assert _token(page.text) == T0

        poll_t0 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        if write_mode == "commit":
            assert poll_t0.status_code == 200
            assert _high_low_values(poll_t0.text)[1:2] == V1
            assert _token(poll_t0.text) == T1

            fresh_page = client.get(f"/forecast?site={seed.site_id}")
            assert fresh_page.status_code == 200
            assert _site_label(fresh_page.text) == "Synthetic Site B"
            assert _high_low_values(fresh_page.text)[1:2] == V1
            assert _token(fresh_page.text) == T1
        else:
            assert poll_t0.status_code == 204

            fresh_page = client.get(f"/forecast?site={seed.site_id}")
            assert fresh_page.status_code == 200
            assert _site_label(fresh_page.text) == "Synthetic Site A"

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
        assert _high_low_values(r1.text)[1:2] == V0
        assert _token(r1.text) == T0

        r2 = client.get(f"/forecast/tiles?site={seed.site_id}&fingerprint={T0}")
        if write_mode == "commit":
            assert r2.status_code == 200
            assert _high_low_values(r2.text)[1:2] == V1
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
            assert _high_low_values(r2.text)[1:2] == V1
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
# T-I: cancellation mid-snapshot rolls back cleanly.
# ---------------------------------------------------------------------------


def test_ti_cancellation_mid_snapshot_rolls_back_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_read_forecast_context is imported INSIDE this test body: a
    module-level import would make the whole file fail to collect on
    57456a0, and the red run could no longer show which cases pass there.
    The blocking rendezvous point is retargeted to
    ``wxverify.forecast.service.effective_blend_depths`` -- the module that
    actually calls it from inside ``load_page_inputs`` (:func:`page.py's
    build_forecast_page` -> ``load_page_inputs`` -> ``effective_blend_depths``);
    ``routes.py`` no longer imports that name itself."""
    from wxverify.forecast import service as service_module
    from wxverify.settings.depth import EffectiveDepth
    from wxverify.web.routes import _read_forecast_context

    db = Database(str(tmp_path / "t_i.db"))
    try:
        # A site must exist, or _load_forecast_context never calls
        # build_forecast_page -- and so never reaches effective_blend_depths,
        # the blocking rendezvous below -- leaving `entered` unset forever.
        _make_site(db._conn, "T-I Site")  # noqa: SLF001
        db._conn.commit()  # noqa: SLF001
        entered = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        recorded: list[bool] = []
        original_depths = service_module.effective_blend_depths

        def _blocking_depths(conn: sqlite3.Connection) -> dict[str, EffectiveDepth]:
            recorded.append(conn.in_transaction)
            entered.set()
            assert release.wait(timeout=2.0)
            return original_depths(conn)

        monkeypatch.setattr(service_module, "effective_blend_depths", _blocking_depths)

        def _cb(conn: sqlite3.Connection) -> dict[str, object]:
            result = _read_forecast_context(conn, None, 1, label="forecast_page")
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
        _load_forecast_context(conn, None, 1)

    with read_snapshot(conn, label="t_k"):
        result = _load_forecast_context(conn, None, 1)
    assert set(result) == {"sites", "site", "page", "day"}


# ---------------------------------------------------------------------------
# T-ros -- the
# query_only reset
# ---------------------------------------------------------------------------


def _init_snapshot_tmp_db(tmp_path: Path) -> Database:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    return init_db(str(db_path))


def _make_snapshot_probe(db: Database) -> None:
    db._conn.execute("CREATE TABLE _probe(a INTEGER)")  # noqa: SLF001


def test_ros_query_only_reset_on_normal_exit_and_after_an_exception(
    tmp_path: Path,
) -> None:
    """T-ros -> at the first `query_only` read: correct = 1 (the pragma is
    ON for the duration of the `read_only_snapshot` block), mutant (a
    `read_only_snapshot` that never sets it) = 0.
    T-ros -> at the INSERT under `query_only`: correct = raises
    `sqlite3.OperationalError` (a read-only connection refuses a write),
    mutant (the pragma never actually set) = the INSERT succeeds silently.
    T-ros -> at `query_only_after` and `query_only_after_exception`: correct
    = 0 both times (the reset in `read_only_snapshot`'s own `finally` runs
    whether the block exited normally or raised), mutant (a reset that only
    runs on the happy path) = 1 after the exception case.
    """
    db = _init_snapshot_tmp_db(tmp_path)
    _make_snapshot_probe(db)

    def _fn(conn: sqlite3.Connection) -> None:
        with read_only_snapshot(conn, label="t"):
            query_only = conn.execute("PRAGMA query_only").fetchone()[0]
            assert query_only == 1
            with pytest.raises(sqlite3.OperationalError):
                conn.execute("INSERT INTO _probe(a) VALUES (1)")
        query_only_after = conn.execute("PRAGMA query_only").fetchone()[0]
        assert query_only_after == 0

        try:
            with read_only_snapshot(conn, label="t2"):
                raise ValueError("boom")
        except ValueError:
            pass
        query_only_after_exception = conn.execute("PRAGMA query_only").fetchone()[0]
        assert query_only_after_exception == 0

    async def _run() -> None:
        await db.read(_fn)

    asyncio.run(_run())

    assert_read_pool_at_rest(db)
