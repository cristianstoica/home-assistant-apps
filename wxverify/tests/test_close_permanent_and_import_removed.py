"""W5 tests (plan 2026-11-02-storage-release.md, table 1.B, 3.2, 5.5, W5).

Names relied on: ``wxverify.scoring.consensus.materialize_consensus``
(``conn, *, site_id, variable, valid_at, now_utc=None``), which deletes no
pair, ``score_cache`` or ``forecast_history`` row on an ``observations``
write or delete (B6); the routes ``PUT /api/feeds/{id}`` (``update_feed``,
B12), ``PUT /api/sites/{id}/feeds/{id}`` (``update_subscription``, B13),
and ``PUT /api/sites/{id}`` (``update_site``, B14, ``sites.py:133-148``),
none of which touch ``forecast_history`` -- the storage-release table drop
(3.1) removed the mean-rebuild/pair-invalidation call path these routes
used to run through;
``wxverify.api.routes.db_transfer`` (``export_begin``/``export_status``/
``export_download``, ``import_db`` at ``:671``, ``run_rebuild_all``); the
Ops panel marker ``id="import-panel"`` in
``web/templates/ops/_import.html``, included at ``ops/show.html:22``; and
``wxverify.api.app``'s startup read of the ``import_rebuild_state`` runtime
key (``api/app.py:192-197``), removed by 5.5 step (d).

Every test here that still exercises a dropped-pair-table trigger pins a
behaviour that already holds with the pair tables gone -- a guard against
a future regression, not a from-the-build-fails-first pin.
``test_export_round_trip_still_opens_and_passes_integrity_check`` is a
separate guard against over-deletion in the W5 build; its docstring says
so.

Real SQLite only (``tests.helpers.asof_conn``/``init_db``, real migrations,
real routes through ``starlette.testclient.TestClient``). Only synthetic
station/site/feed names (``obs-write-site``, ``feed-put-model``, ...), the
``UTC`` zone and the fixed synthetic date ``tests.helpers.D`` appear anywhere
in this file (public repo).
"""

from __future__ import annotations

import gzip
import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from starlette.testclient import TestClient

from tests.helpers import (
    NOW,
    D,
    asof_conn,
    asof_make_real_feed,
    asof_make_site,
    seed_forecast_history,
)
from wxverify import config
from wxverify.api.app import create_app
from wxverify.api.routes import db_transfer
from wxverify.db.connection import close_db, init_db
from wxverify.db.runtime_state import set_runtime_state
from wxverify.scoring.consensus import materialize_consensus

_EXPORT_BASE = "/api/export"


# ---------------------------------------------------------------------------
# Shared harness (local to this file; nothing added to tests/helpers.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    import asyncio

    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


def _rows(
    conn: sqlite3.Connection, sql: str, params: tuple[object, ...]
) -> list[dict[str, object]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


@pytest.fixture(autouse=True)
def _reset_exports() -> Iterator[None]:
    """Isolate the module-global ``_EXPORTS`` registry across tests here."""

    def _clear() -> None:
        for job in list(db_transfer._EXPORTS.values()):  # noqa: SLF001
            db_transfer._unlink(job.path)  # noqa: SLF001
        db_transfer._EXPORTS.clear()  # noqa: SLF001

    _clear()
    yield
    _clear()


def _await_export_ready(
    client: TestClient, export_id: str, *, tries: int = 50
) -> dict[str, Any]:
    for _ in range(tries):
        resp = client.get(f"{_EXPORT_BASE}/status/{export_id}")
        assert resp.status_code == 200, f"status: {resp.status_code} {resp.text}"
        data: dict[str, Any] = resp.json()
        if data["state"] != "preparing":
            return data
        time.sleep(0.05)
    raise AssertionError(f"export {export_id} never left 'preparing'")


# ---------------------------------------------------------------------------
# 1. An ``observations`` write (B6) deletes no pair/cache/history row.
# ---------------------------------------------------------------------------


def test_observations_write_deletes_no_pair_cache_or_history_row() -> None:
    """B6: ``materialize_consensus`` must not delete any ``forecast_history``
    row on an ``observations`` write (no pair tables are read or written
    here at all any more, storage-release plan table 1.B row B6). Guards
    against a future regression reintroducing a dependents-invalidation
    delete keyed on this write.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "obs-write-site")
    feed_id = asof_make_real_feed(conn, "obs-write-model")
    valid_at = f"{D.isoformat()}T06:00:00Z"

    seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=D,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_at,
        lead_hours=6,
        forecast=5.0,
        observed=4.0,
    )
    conn.commit()

    hist_sql = (
        "SELECT * FROM forecast_history WHERE site_id=? AND variable=? AND valid_at=?"
    )

    before_hist = _rows(conn, hist_sql, (site_id, "temperature", valid_at))
    assert before_hist

    materialize_consensus(
        conn, site_id=site_id, variable="temperature", valid_at=valid_at, now_utc=NOW
    )
    conn.commit()

    assert before_hist == _rows(conn, hist_sql, (site_id, "temperature", valid_at))


# ---------------------------------------------------------------------------
# 2. Feed PUT (B12), subscription toggle (B13) and rain-threshold PUT (B14)
#    each touch no pair table.
# ---------------------------------------------------------------------------


def test_feed_put_leaves_pair_tables_as_seeded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B12: ``PUT /api/feeds/{id}`` toggling ``enabled`` must not touch the
    site's ``forecast_history`` rows, for the toggled feed or the
    multimodel-mean feed, no pair table remaining to invalidate any more
    (storage-release plan table 1.B rows B12/B13).
    """
    conn = _init_tmp_db(tmp_path)
    site_id = asof_make_site(conn, "feed-put-site")
    feed_id = asof_make_real_feed(conn, "feed-put-model")
    valid_at = f"{D.isoformat()}T06:00:00Z"
    mean_feed_row = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchone()
    assert mean_feed_row is not None
    mean_feed_id = int(mean_feed_row["id"])
    seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=D,
        feed_id=mean_feed_id,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_at,
        lead_hours=6,
        forecast=4.5,
        observed=4.0,
    )
    seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=D,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_at,
        lead_hours=6,
        forecast=5.0,
        observed=4.0,
    )
    conn.commit()

    mean_sql = (
        "SELECT * FROM forecast_history WHERE site_id=? AND feed_id=? AND valid_at=?"
    )
    hist_sql = (
        "SELECT * FROM forecast_history WHERE site_id=? AND feed_id=? AND valid_at=?"
    )
    before_mean = _rows(conn, mean_sql, (site_id, mean_feed_id, valid_at))
    before_hist = _rows(conn, hist_sql, (site_id, feed_id, valid_at))
    assert before_mean and before_hist

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.put(
            f"/api/feeds/{feed_id}", json={"enabled": False}, headers=headers
        )
        assert resp.status_code == 200, (resp.status_code, resp.text)

    # The app's lifespan closes the connection on TestClient exit; read the
    # result through a fresh connection to the same file.
    direct = sqlite3.connect(config.db_path)
    direct.row_factory = sqlite3.Row
    try:
        after_mean = _rows(direct, mean_sql, (site_id, mean_feed_id, valid_at))
        after_hist = _rows(direct, hist_sql, (site_id, feed_id, valid_at))
    finally:
        direct.close()
    assert before_mean == after_mean
    assert before_hist == after_hist


def test_subscription_toggle_leaves_pair_tables_as_seeded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B13: ``PUT /api/sites/{id}/feeds/{id}`` (``update_subscription``)
    must not touch the site's ``forecast_history`` rows, for the toggled
    feed or the multimodel-mean feed, no pair table remaining to
    invalidate any more (storage-release plan table 1.B row B13).
    """
    conn = _init_tmp_db(tmp_path)
    site_id = asof_make_site(conn, "sub-toggle-site")
    feed_id = asof_make_real_feed(conn, "sub-toggle-model")
    valid_at = f"{D.isoformat()}T06:00:00Z"
    mean_feed_row = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchone()
    assert mean_feed_row is not None
    mean_feed_id = int(mean_feed_row["id"])
    seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=D,
        feed_id=mean_feed_id,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_at,
        lead_hours=6,
        forecast=4.5,
        observed=4.0,
    )
    seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=D,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_at,
        lead_hours=6,
        forecast=5.0,
        observed=4.0,
    )
    conn.commit()

    mean_sql = (
        "SELECT * FROM forecast_history WHERE site_id=? AND feed_id=? AND valid_at=?"
    )
    hist_sql = (
        "SELECT * FROM forecast_history WHERE site_id=? AND feed_id=? AND valid_at=?"
    )
    before_mean = _rows(conn, mean_sql, (site_id, mean_feed_id, valid_at))
    before_hist = _rows(conn, hist_sql, (site_id, feed_id, valid_at))
    assert before_mean and before_hist

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.put(
            f"/api/sites/{site_id}/feeds/{feed_id}",
            json={"enabled": True},
            headers=headers,
        )
        assert resp.status_code == 200, (resp.status_code, resp.text)

    direct = sqlite3.connect(config.db_path)
    direct.row_factory = sqlite3.Row
    try:
        after_mean = _rows(direct, mean_sql, (site_id, mean_feed_id, valid_at))
        after_hist = _rows(direct, hist_sql, (site_id, feed_id, valid_at))
    finally:
        direct.close()
    assert before_mean == after_mean
    assert before_hist == after_hist


def test_rain_threshold_put_leaves_pair_tables_as_seeded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B14: ``PUT /api/sites/{id}`` with a new ``rain_threshold_mm``
    (``sites.py:133-148``) must not touch the site's precip
    ``forecast_history`` rows, no pair table remaining to re-pair
    (storage-release plan table 1.B row B14).
    """
    conn = _init_tmp_db(tmp_path)
    site_id = asof_make_site(conn, "rain-threshold-site")
    feed_id = asof_make_real_feed(conn, "rain-threshold-model")
    valid_at = f"{D.isoformat()}T12:00:00Z"
    seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=D,
        feed_id=feed_id,
        variable="precip",
        day_ahead=1,
        valid_at=valid_at,
        lead_hours=6,
        forecast=0.3,
        observed=0.2,
    )
    conn.commit()

    hist_sql = (
        "SELECT * FROM forecast_history WHERE site_id=? AND feed_id=?"
        " AND variable='precip' AND valid_at=?"
    )
    before_hist = _rows(conn, hist_sql, (site_id, feed_id, valid_at))
    assert before_hist

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.put(
            f"/api/sites/{site_id}", json={"rain_threshold_mm": 1.0}, headers=headers
        )
        assert resp.status_code == 200, (resp.status_code, resp.text)

    direct = sqlite3.connect(config.db_path)
    direct.row_factory = sqlite3.Row
    try:
        after_hist = _rows(direct, hist_sql, (site_id, feed_id, valid_at))
    finally:
        direct.close()
    assert before_hist == after_hist


# ---------------------------------------------------------------------------
# 4/5/6. The database import is gone; export still works; start-up no
#         longer reads import_rebuild_state.
# ---------------------------------------------------------------------------


def test_import_db_route_is_gone_and_ops_page_has_no_import_panel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """5.5: ``POST /api/import/db`` (``api/routes/db_transfer.py:671``) and
    the Ops import panel (``web/templates/ops/_import.html``, included at
    ``ops/show.html:22``, marker ``id="import-panel"``) are both removed
    whole. Fails now: the route still exists (the CSRF guard refuses an
    unauthenticated POST with 403, never 404/405) and the panel still
    renders on ``/ops``.
    """
    _init_tmp_db(tmp_path)
    db_dir = Path(config.db_path).parent
    before_listing = sorted(p.name for p in db_dir.iterdir())
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.post(
            "/api/import/db", content=b"not-a-real-sqlite-file", headers=headers
        )
        assert resp.status_code in (404, 405), (resp.status_code, resp.text)
        ops_html = client.get("/ops").text
    after_listing = sorted(p.name for p in db_dir.iterdir())
    assert set(after_listing) - set(before_listing) == set(), "no upload must be stored"
    assert 'id="import-panel"' not in ops_html


def test_export_round_trip_still_opens_and_passes_integrity_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards against over-deletion from the 5.5 import removal: the
    export half (``begin`` -> ``status`` -> ``download``) must keep
    working. This test MAY ALREADY PASS before the build -- it pins a
    behavior the plan says stays, not a bug the plan fixes; that is
    expected and documented here, per the brief.
    """
    conn = _init_tmp_db(tmp_path)
    asof_make_site(conn, "Export Round Trip Site")
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=headers)
        assert resp.status_code == 202, (resp.status_code, resp.text)
        export_id = resp.json()["export_id"]
        final = _await_export_ready(client, export_id)
        assert final["state"] == "ready"
        dl = client.get(f"{_EXPORT_BASE}/download/{export_id}")
        assert dl.status_code == 200, (dl.status_code, dl.text)
        content = dl.content

    out = tmp_path / "downloaded.db"
    out.write_bytes(gzip.decompress(content))
    exported = sqlite3.connect(f"file:{out}?mode=ro", uri=True)
    try:
        assert exported.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        exported.close()


def test_startup_no_longer_resumes_import_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Start-up never reads ``import_rebuild_state`` and never calls
    ``db_transfer.run_rebuild_all`` because of it: with the key seeded
    ``pending`` (an interrupted rebuild), the lifespan makes no rebuild
    call and the key is left as it stands, so the call count -- not the
    stored value -- is the observable this pins.
    """
    conn = _init_tmp_db(tmp_path)
    set_runtime_state(conn, "import_rebuild_state", "pending")
    conn.commit()
    close_db()

    calls: list[None] = []

    async def _tracking_rebuild() -> None:
        calls.append(None)

    monkeypatch.setattr(
        db_transfer, "run_rebuild_all", _tracking_rebuild, raising=False
    )
    app = _make_app(monkeypatch)
    with TestClient(app):
        pass

    assert calls == [], (
        "W5 removes the startup rebuild-resume read (api/app.py:192-197)"
    )
    direct = sqlite3.connect(config.db_path)
    try:
        row = direct.execute(
            "SELECT value FROM runtime_state WHERE key='import_rebuild_state'"
        ).fetchone()
    finally:
        direct.close()
    assert row is not None and row[0] == "pending"
