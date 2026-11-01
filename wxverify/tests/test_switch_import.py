"""Database import after the switch (daily-close switch-over plan §11; test
IDs IM1-IM4 of §17.11).

Written before P6's import rule exists: today's ``run_rebuild_all`` always
rebuilds (``_rebuild_all``'s delete-all-and-re-pair path, R18), and today's
import neutralizer (``_neutralize_imported_verification_chains``) marks
retired-kind jobs ``failed`` instead of deleting them, with its own
``IMPORT_SUPPRESSED_REASON`` text instead of P6's switch-specific message.
Every row below fails today on a plain assertion against that unchanged
behaviour.

Upload files are built the way ``tests/test_db_transfer.py``'s own v8
fixture is: a real, fully-migrated ``Database`` (not a byte-faithful old
file), with its ``PRAGMA user_version`` set directly to the version under
test -- the same "fake it at the version pragma" convention
``make_v8_wind_state``/``_sqlite_bytes_with_user_version`` already use in
this package. Synthetic data only: no real station id, coordinate, site or
zone name.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import asof_make_real_feed, asof_make_site
from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import Database, close_db, init_db
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.switch import ensure_site_generation

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_db_transfer.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path, name: str) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / f"{name}.db"
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
    csrf = client.get("/api/csrf").json()["csrf_token"]
    return {
        "Origin": "http://testserver",
        "X-CSRF-Token": csrf,
        "Content-Type": "application/octet-stream",
    }


def _seed_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    generation_id: int,
) -> None:
    conn.execute(
        "INSERT INTO forecast_pairs"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours,"
        "  day_ahead, forecast, observed, tz_generation_id)"
        " VALUES (?, ?, 'temperature', '2024-01-01T00:00:00Z',"
        "  '2024-01-02T00:00:00Z', 24, 1, 10.0, 11.0, ?)",
        (site_id, feed_id, generation_id),
    )


def _build_pre_switch_upload(
    tmp_path: Path, name: str, *, with_catchup_job: bool
) -> Path:
    """A fully-migrated DB, one site with one stored pair, its user_version
    forced to 10 -- a pre-switch file below version 15 (plan §17.1's
    "the pre-switch file")."""
    path = tmp_path / f"{name}.db"
    db = Database(str(path))
    try:
        conn = db._conn  # noqa: SLF001
        site_id = asof_make_site(conn, f"{name}-site")
        feed_id = asof_make_real_feed(conn, f"{name}-feed")
        generation_id = ensure_site_generation(conn, site_id)
        _seed_pair(conn, site_id=site_id, feed_id=feed_id, generation_id=generation_id)
        if with_catchup_job:
            conn.execute(
                "INSERT INTO jobs (type, site_id, job_key) VALUES"
                " ('catchup', NULL, 'catchup')"
            )
        conn.commit()
        conn.execute("PRAGMA user_version = 10")
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        db.close()
    return path


def test_im1_import_of_a_pre_switch_file_keeps_its_pairs_and_drops_catchup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    upload_path = _build_pre_switch_upload(
        tmp_path, "im1-upload", with_catchup_job=True
    )
    payload = upload_path.read_bytes()

    _init_tmp_db(tmp_path, "im1-live")
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        with caplog.at_level("INFO"):
            resp = client.post("/api/import/db", content=payload, headers=headers)
    assert resp.status_code == 200, resp.text

    direct = sqlite3.connect(config.db_path)
    direct.row_factory = sqlite3.Row
    try:
        pair_count = direct.execute("SELECT COUNT(*) FROM forecast_pairs").fetchone()[0]
        assert pair_count == 1, (
            "M65 -> at this assertion: correct = 1 (the uploaded pair kept"
            " as-is, no rebuild), mutant = "
            f"{pair_count} (run_rebuild_all rebuilt a file without the key:"
            " writable_pairs_clause returns '1' with the close off, so"
            " _rebuild_all deletes every forecast_pairs row, then re-pairs"
            " from the station data it has -- none here -- leaving the"
            " table empty)"
        )

        catchup_rows = direct.execute(
            "SELECT COUNT(*) FROM jobs WHERE type = 'catchup'"
        ).fetchone()[0]
        assert catchup_rows == 0, (
            f"correct = 0 (the catchup row settled), found {catchup_rows}"
        )
    finally:
        direct.close()

    info_lines = [r.getMessage() for r in caplog.records if r.levelname == "INFO"]
    assert any("its history is kept as it stands" in line for line in info_lines), (
        "M65 -> at this assertion: correct = the INFO line 'import: file"
        " from before the daily close; its history is kept as it stands'"
        " logged once, mutant = no such line (the skip-the-rebuild branch"
        " never ran); lines seen: "
        f"{info_lines!r}"
    )


def test_im2_import_of_a_post_switch_file_the_gate_calls_no_advance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """IM2: a post-switch file (the key already set) imports with its own
    state intact, and the lane's first claim comes with no ``Cutover``
    construction. Imports ``wxverify.worker.cutover`` inside its own body
    (P6's module, not yet present): today this fails at that import.
    """
    from wxverify.db.site_days import DAILY_CLOSE_ON_KEY
    from wxverify.worker.cutover import Cutover  # noqa: F401 -- P6's module

    path = tmp_path / "im2-upload.db"
    db = Database(str(path))
    try:
        conn = db._conn  # noqa: SLF001
        asof_make_site(conn, "im2-site")
        set_runtime_state(conn, DAILY_CLOSE_ON_KEY, "2026-01-01T00:00:00Z")
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        db.close()
    payload = path.read_bytes()

    import wxverify.worker.processor as processor_module
    from wxverify.db.queue import enqueue_if_absent

    class _Stop(Exception):
        pass

    class _CountingCutover:
        instances = 0

        def __init__(self, db_arg: Any) -> None:
            type(self).instances += 1

        async def advance(self) -> Any:
            raise _Stop

    monkeypatch.setattr(processor_module, "Cutover", _CountingCutover)
    monkeypatch.setattr(processor_module, "POLL_INTERVAL", 0)

    async def _stop_on_claim(db_arg: Any, job: Any, *, lane: str) -> None:
        raise _Stop

    monkeypatch.setattr(processor_module, "run_claimed_job", _stop_on_claim)

    _init_tmp_db(tmp_path, "im2-live")
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
        assert resp.status_code == 200, resp.text

        live_conn = sqlite3.connect(config.db_path)
        live_conn.row_factory = sqlite3.Row
        try:
            site_id = int(
                live_conn.execute(
                    "SELECT id FROM sites WHERE name = 'im2-site'"
                ).fetchone()["id"]
            )
        finally:
            live_conn.close()

        from wxverify.db.connection import get_db

        get_db().write_sync(
            lambda conn: enqueue_if_absent(conn, "fetch_feed", site_id, "im2-feed", {})
        )

        with pytest.raises(_Stop):
            asyncio.run(processor_module._run_main_lane(get_db()))

    assert _CountingCutover.instances == 0, (
        f"M66 -> at this assertion: correct = 0 Cutover instances (the gate"
        f" calls advance with the key set), mutant = {_CountingCutover.instances}"
    )


def test_im3_boot_resumes_a_pending_rebuild_with_no_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """IM3: a pre-switch file whose ``import_rebuild_state`` is ``pending``
    at boot resumes to ``done`` with no rebuild, before the worker starts."""
    close_db()
    db_path = tmp_path / "im3.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)

    db = Database(str(db_path))
    try:
        conn = db._conn  # noqa: SLF001
        site_id = asof_make_site(conn, "im3-site")
        feed_id = asof_make_real_feed(conn, "im3-feed")
        generation_id = ensure_site_generation(conn, site_id)
        _seed_pair(conn, site_id=site_id, feed_id=feed_id, generation_id=generation_id)
        set_runtime_state(conn, "import_rebuild_state", "pending")
        conn.commit()
        conn.execute("PRAGMA user_version = 10")
        conn.commit()
    finally:
        db.close()

    app = _make_app(monkeypatch)
    with caplog.at_level("INFO"), TestClient(app):
        pass  # lifespan startup/shutdown alone resumes the rebuild.

    direct = sqlite3.connect(config.db_path)
    direct.row_factory = sqlite3.Row
    try:
        state = direct.execute(
            "SELECT value FROM runtime_state WHERE key = 'import_rebuild_state'"
        ).fetchone()[0]
        assert state == "done"
        pair_count = direct.execute("SELECT COUNT(*) FROM forecast_pairs").fetchone()[0]
        assert pair_count == 1, (
            "M65 -> at this assertion: correct = 1 (the boot resume skips"
            " the rebuild for a file without the key, same as IM1), mutant"
            f" = {pair_count}"
        )
    finally:
        direct.close()


def test_im4_import_fails_the_unfinished_run_and_deletes_its_jobs_and_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """IM4: a pre-switch file carrying one pending and one running
    ``verification_run`` job, an unfinished (``running``) run with
    worksheet rows, and the site's ``verification_state:``/
    ``verification_heartbeat:`` keys."""
    path = tmp_path / "im4-upload.db"
    db = Database(str(path))
    try:
        conn = db._conn  # noqa: SLF001
        site_id = asof_make_site(conn, "im4-site")
        generation_id = ensure_site_generation(conn, site_id)
        conn.execute(
            "INSERT INTO jobs (type, site_id, job_key, status) VALUES"
            " ('verification_run', ?, 'verify', 'pending')",
            (site_id,),
        )
        conn.execute(
            "INSERT INTO jobs (type, site_id, job_key, status) VALUES"
            " ('verification_run', ?, 'verify-running', 'running')",
            (site_id,),
        )
        run_id = conn.execute(
            "INSERT INTO verification_runs"
            " (site_id, tz_generation_id, methodology_version, app_version,"
            "  state, attempt, config_snapshot, bootstrap_seed,"
            "  bootstrap_resamples, input_fingerprint)"
            " VALUES (?, ?, 4, 'test', 'running', 1, '{}', 0, 0, 'fp')",
            (site_id, generation_id),
        ).lastrowid
        conn.execute(
            "INSERT INTO verification_evidence"
            " (run_id, snapshot_local_date, target_local_date, lead, variable,"
            "  quantity, entity_type, entity_key, forecast_eligible,"
            "  truth_eligible)"
            " VALUES (?, '2024-01-01', '2024-01-01', 0, 'temperature',"
            "  'temperature_high', 'feed', 'x', 0, 0)",
            (run_id,),
        )
        set_runtime_state(conn, f"verification_state:{site_id}", "running")
        set_runtime_state(
            conn, f"verification_heartbeat:{site_id}", "2024-01-01T00:00:00Z"
        )
        conn.commit()
        conn.execute("PRAGMA user_version = 10")
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        db.close()
    payload = path.read_bytes()

    _init_tmp_db(tmp_path, "im4-live")
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
    assert resp.status_code == 200, resp.text

    direct = sqlite3.connect(config.db_path)
    direct.row_factory = sqlite3.Row
    try:
        imported_site = direct.execute(
            "SELECT id FROM sites WHERE name = 'im4-site'"
        ).fetchone()
        imported_site_id = int(imported_site["id"])

        job_count = direct.execute(
            "SELECT COUNT(*) FROM jobs WHERE type = 'verification_run'"
        ).fetchone()[0]
        assert job_count == 0, (
            "M72 -> at this assertion: correct = 0 verification_run job"
            " rows of any status (A2 deletes them, it does not mark them"
            " failed), mutant = "
            f"{job_count} (the import neutralizer kept: it marks them"
            " 'failed' instead of deleting them)"
        )

        run_row = direct.execute(
            "SELECT state, error FROM verification_runs WHERE site_id = ?",
            (imported_site_id,),
        ).fetchone()
        assert run_row is not None
        assert run_row["state"] == "failed"
        assert run_row["error"] == "retired at the switch to the daily close", (
            "M72 -> at this assertion: correct = the error"
            " 'retired at the switch to the daily close' (P6's own wording),"
            f" mutant = {run_row['error']!r} (the import neutralizer kept,"
            " with its own IMPORT_SUPPRESSED_REASON wording)"
        )

        for key in ("verification_state", "verification_heartbeat"):
            row = direct.execute(
                "SELECT value FROM runtime_state WHERE key = ?",
                (f"{key}:{imported_site_id}",),
            ).fetchone()
            assert row is None, f"{key}:{imported_site_id} must be gone"
    finally:
        direct.close()
