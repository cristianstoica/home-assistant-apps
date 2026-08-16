"""Retrospective timezone-correction Ops-control-surface oracles (0.13.0,
plan §10 O1-O16).

Covers the ``POST /api/sites/{id}/timezone-correction`` handler's refusal
mapping (§7's exhaustive by-exception-type table), its ``MutationGuard``
exposure, the loader/route applicability agreement (D6/D8), the panel's
per-site query shape (O9), successful start (O10), non-mutation of published
history until the flip (O11), the finished/failed/cleanup-stalled surfaces
(O12/O13/O14/O16), the always-visible disclosure copy (O15), and the
stalled-cleanup predicate's single-statement snapshot plus its full
committed-state truth table (O17).

Isolation: HTTP-route oracles drive a real app over ``TestClient`` against a
file-backed ``tmp_path`` database with an idle worker stand-in, mirroring
``tests/test_publish_hold_control.py``. Loader/query-shape oracles (O8's
direct-loader half, O9) drive an in-memory connection directly, mirroring
``tests/test_tz_correction_oracles.py``. Chain-driving oracles (O12-O14,
O16) seed and drive through a file-backed app's own connection via
``db.write_sync``, so the intermediate states they inspect (job row status,
runtime_state blob) are exactly what the real worker would leave behind.

Terminal job failure (O13, O16) is always forced through the ``jobs`` row
(``UPDATE jobs SET max_retries = 0 WHERE id = ?``) then ``_fail_job``, never
by constructing a ``Job`` object directly -- ``fail()`` re-reads
``retry_count``/``max_retries`` from the row (``db/queue.py``), never from
the caller's object.

Synthetic data only (public repo): ``site-alpha`` / ``America/Denver``,
correction target ``Etc/GMT+7``, coordinates 40.0/-105.0, no real station,
city, or device identifiers.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.helpers import (
    asof_conn,
    asof_insert_observation,
    asof_insert_sample,
    asof_make_real_feed,
    asof_make_site,
)
from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db
from wxverify.db.queue import claim_next_job
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.tz_generations import (
    correction_job_key,
    ensure_published_generation,
    published_generation_clause,
    published_generation_id,
    published_pointer_key,
)
from wxverify.scoring.pairing import pair_real_models
from wxverify.scoring.persistence import materialize_persistence
from wxverify.web.context import load_ops, load_sites, load_timezone_correction
from wxverify.worker.processor import _complete_and_continue, _fail_job
from wxverify.worker.tz_correction import (
    advance_correction,
    build_continuation,
    correction_state_key,
)
from wxverify.worker.verification_run import verification_job_key

_SITE_NAME = "site-alpha"
_SITE_TZ = "America/Denver"
_NEW_TZ = "Etc/GMT+7"
_POST_PATH = "/api/sites/{site_id}/timezone-correction"


# ---------------------------------------------------------------------------
# Shared HTTP-surface helpers (mirrors tests/test_publish_hold_control.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _boot(tmp_path: Path, name: str, monkeypatch: pytest.MonkeyPatch) -> object:
    close_db()
    config.db_path = str(tmp_path / name)
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


def _insert_site(
    conn: sqlite3.Connection, name: str = _SITE_NAME, *, enabled: int = 1
) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites
            (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
        VALUES (?, 40.0, -105.0, 1600.0, ?, ?)
        """,
        (name, _SITE_TZ, enabled),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_verification_job(
    conn: sqlite3.Connection, site_id: int, *, status: str = "pending"
) -> None:
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES "
        "('verification_run', ?, ?, ?)",
        (site_id, verification_job_key(site_id), status),
    )


def _building_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM timezone_generations WHERE state='building'"
    ).fetchone()
    return int(row["n"])


def _correction_job_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type='timezone_correction'"
    ).fetchone()
    return int(row["n"])


def _activity_counts(conn: sqlite3.Connection) -> tuple[int, int]:
    return (_building_count(conn), _correction_job_count(conn))


def _norm(text: str) -> str:
    """Collapse the template's line-wrapped whitespace for substring checks."""
    return " ".join(text.split())


# ---------------------------------------------------------------------------
# Chain-driving helper (mirrors worker/processor.py's own dispatch exactly,
# via the real queue -- claim -> advance -> continuation -> complete).
# ---------------------------------------------------------------------------


def _drain_foreign_jobs(conn: sqlite3.Connection) -> None:
    """Complete any pending ``pair_and_score`` job -- the flip's own known
    side effect, not this test's correction chain.

    The flip chunk (``worker/tz_correction.py: _flip``) enqueues a
    ``pair_and_score`` job in the SAME transaction as the flip -- a real,
    expected side effect, not a fixture leak. ``claim_next_job`` claims
    globally by type-priority tier (db/queue.py:276-280), and
    ``pair_and_score`` outranks ``timezone_correction`` (the LAST tier), so
    it would otherwise be claimed ahead of the chain's own next job on every
    drive step following a flip. We don't care about its effects for these
    oracles -- only that it stops shadowing the correction chain's own
    claims -- so it is drained by direct completion rather than by running
    real scoring logic.

    Deliberately an allowlist of ``pair_and_score`` (the one job type this
    drain exists to clear), not a denylist of ``timezone_correction``: with
    a denylist, a future change that made the correction chain enqueue some
    third job type would have that job silently completed by this harness
    instead of the chain claiming it; with an allowlist that third job stays
    pending, gets claimed as an unexpected job, and fails the chain-identity
    assertion in ``_claim_and_step`` loudly with a legible key.
    """
    while True:
        row = conn.execute(
            "SELECT id FROM jobs WHERE status='pending' "
            "AND type = 'pair_and_score' ORDER BY id LIMIT 1"
        ).fetchone()
        if row is None:
            return
        _complete_and_continue(conn, int(row["id"]), None)


def _claim_and_step(conn: sqlite3.Connection, site_id: int, generation_id: int) -> bool:
    _drain_foreign_jobs(conn)
    job = claim_next_job(conn)
    assert job is not None, "expected a claimable timezone_correction job"
    # Trap #2: timezone_correction is the LAST claim-priority tier
    # (db/queue.py:276-280); assert identity so a stray pending row from a
    # DIFFERENT fixture could never be silently claimed instead. Foreign
    # job types (e.g. the flip's own pair_and_score side effect) are
    # drained above, so this now catches only a genuine identity mismatch.
    assert job.job_key == correction_job_key(generation_id), (
        f"claimed {job.job_key!r}, expected {correction_job_key(generation_id)!r}"
    )
    more = advance_correction(conn, site_id, job.payload)
    continuation = build_continuation(site_id, job.payload) if more else None
    _complete_and_continue(conn, job.id, continuation)
    return more


def _drive_to_state(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    *,
    stop_state: str,
    max_steps: int = 500,
) -> None:
    for _ in range(max_steps):
        _claim_and_step(conn, site_id, generation_id)
        row = conn.execute(
            "SELECT state FROM timezone_generations WHERE id=?", (generation_id,)
        ).fetchone()
        assert row is not None
        if str(row["state"]) == stop_state:
            return
    raise AssertionError(f"correction chain never reached state={stop_state!r}")


def _drive_to_complete(
    conn: sqlite3.Connection, site_id: int, generation_id: int, *, max_steps: int = 500
) -> None:
    for _ in range(max_steps):
        if not _claim_and_step(conn, site_id, generation_id):
            return
    raise AssertionError("correction chain did not terminate")


def _seed_correctable_history(conn: sqlite3.Connection) -> int:
    """Synthetic two-day history whose real-model pairing produces both
    changed and unchanged day-ahead buckets under ``_NEW_TZ`` (adapted from
    ``tests/test_tz_correction.py:_seed_history`` for this file's site
    name/timezone).
    """
    site_id = _insert_site(conn)
    feed_id = asof_make_real_feed(conn, "model-a")
    hours = (
        "2026-06-10T18:00:00Z",
        "2026-06-10T22:00:00Z",
        "2026-06-10T23:00:00Z",
        "2026-06-11T00:00:00Z",
    )
    for index, hour in enumerate(hours):
        asof_insert_observation(
            conn,
            site_id=site_id,
            valid_at=hour,
            value=10.0 + index,
            computed_at="2026-06-11T01:00:00Z",
        )
    samples = (
        ("2026-06-10T00:00:00Z", "2026-06-10T22:00:00Z", 22),
        ("2026-06-10T00:00:00Z", "2026-06-10T23:00:00Z", 23),
        ("2026-06-09T12:00:00Z", "2026-06-10T23:00:00Z", 35),
        ("2026-06-10T12:00:00Z", "2026-06-10T18:00:00Z", 6),
    )
    for issued, valid, lead in samples:
        asof_insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            issued_at=issued,
            valid_at=valid,
            lead_hours=lead,
            value=11.5,
            fetched_at=issued,
        )
    ensure_published_generation(conn, site_id)
    pair_real_models(conn, site_id)
    materialize_persistence(conn, site_id)
    return site_id


def _seed_min_history(conn: sqlite3.Connection, site_id: int) -> None:
    """One observation, just enough for ``ensure_published_generation`` plus
    a claimable job -- used only where the chain is forced to fail BEFORE
    any chunk runs (O13), so no rebuildable data is needed.
    """
    asof_insert_observation(
        conn,
        site_id=site_id,
        valid_at="2026-06-10T18:00:00Z",
        value=10.0,
        computed_at="2026-06-10T19:00:00Z",
    )
    ensure_published_generation(conn, site_id)


# ---------------------------------------------------------------------------
# O1 - Consent gate: refused before any write, at the status its own layer
# owns (schema vs. route).
# ---------------------------------------------------------------------------


def test_o1_omitted_confirm_is_rejected_by_the_schema_before_any_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o1-missing.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        headers = _csrf_headers(client)

        resp = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ},
            headers=headers,
        )

        assert resp.status_code == 422, resp.text
        body = resp.json()
        assert body["error"] == "validation failed"
        detail = body["detail"]
        assert any("confirm" in error["loc"] for error in detail), detail
        counts = db.read_sync(_activity_counts)
        assert counts == (0, 0)
    close_db()


def test_o1_confirm_false_is_rejected_by_the_route_before_any_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o1-false.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        headers = _csrf_headers(client)

        resp = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": False},
            headers=headers,
        )

        assert resp.status_code == 400, resp.text
        assert resp.json() == {"error": "confirmation required"}
        counts = db.read_sync(_activity_counts)
        assert counts == (0, 0)
    close_db()


# ---------------------------------------------------------------------------
# O2 / O3 - Unknown timezone vs. unknown site: distinguishable statuses and
# messages (single test proves distinguishability, per plan).
# ---------------------------------------------------------------------------


def test_o2_and_o3_unknown_timezone_and_unknown_site_are_distinguishable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o2-o3.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        headers = _csrf_headers(client)

        bad_zone = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": "Nope/Nope", "confirm": True},
            headers=headers,
        )
        bad_site = client.post(
            _POST_PATH.format(site_id=999999),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )

        assert bad_zone.status_code == 400, bad_zone.text
        assert bad_zone.json() == {"error": "unknown IANA timezone 'Nope/Nope'"}
        assert bad_site.status_code == 404, bad_site.text
        assert bad_site.json() == {"error": "site 999999 does not exist"}
        assert bad_zone.status_code != bad_site.status_code

        counts = db.read_sync(_activity_counts)
        assert counts == (0, 0)
    close_db()


# ---------------------------------------------------------------------------
# O4 - Already-building refusal names the existing generation and leaves
# exactly one building row / one job row (not testing queue-level dedupe).
# ---------------------------------------------------------------------------


def test_o4_second_correction_is_refused_and_names_the_existing_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o4.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        headers = _csrf_headers(client)

        first = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )
        assert first.status_code == 200, first.text
        generation_id = first.json()["generation_id"]

        second = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": "Etc/GMT+8", "confirm": True},
            headers=headers,
        )

        assert second.status_code == 409, second.text
        assert second.json() == {
            "error": (
                f"site {site_id} already has a correction building "
                f"(generation {generation_id})"
            )
        }
        counts = db.read_sync(_activity_counts)
        assert counts == (1, 1)
    close_db()


# ---------------------------------------------------------------------------
# O5 - Route-owned refusal (D6): an active verification chain blocks a
# correction from ever being enqueued.
# ---------------------------------------------------------------------------


def test_o5_active_verification_chain_refuses_the_correction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o5.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        db.write_sync(lambda conn: _insert_verification_job(conn, site_id))
        headers = _csrf_headers(client)

        resp = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )

        assert resp.status_code == 409, resp.text
        assert resp.json() == {
            "error": "a verification run is active for this site; correction refused"
        }
        counts = db.read_sync(_activity_counts)
        assert counts == (0, 0)
    close_db()


# ---------------------------------------------------------------------------
# O6 - No message-text catch-all: an unmapped ValueError propagates as a 500,
# not a guessed refusal status.
# ---------------------------------------------------------------------------


def test_o6_unmapped_domain_error_is_not_swallowed_as_a_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o6.db", monkeypatch)

    def _boom(_conn: object, _site_id: object, _timezone: object) -> int:
        raise ValueError("something else")

    monkeypatch.setattr(
        "wxverify.api.routes.sites.start_retrospective_correction", _boom
    )
    with TestClient(app, raise_server_exceptions=False) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        headers = _csrf_headers(client)

        resp = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )

        assert resp.status_code not in (400, 404, 409), resp.text
        assert resp.status_code == 500, resp.text
    close_db()


# ---------------------------------------------------------------------------
# O7 - MutationGuard coverage: no route-level exemption.
# ---------------------------------------------------------------------------


def test_o7_foreign_origin_is_rejected_and_leaves_the_site_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o7-origin.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        token = client.get("/api/csrf").json()["csrf_token"]

        resp = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers={"Origin": "http://192.0.2.10", "X-CSRF-Token": token},
        )

        assert resp.status_code == 403, resp.text
        assert db.read_sync(_building_count) == 0
    close_db()


def test_o7_disallowed_content_type_is_rejected_and_leaves_the_site_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o7-ctype.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        token = client.get("/api/csrf").json()["csrf_token"]

        resp = client.post(
            _POST_PATH.format(site_id=site_id),
            content=b'{"timezone": "Etc/GMT+7", "confirm": true}',
            headers={
                "Origin": "http://testserver",
                "X-CSRF-Token": token,
                "Content-Type": "text/plain",
            },
        )

        assert resp.status_code == 415, resp.text
        assert db.read_sync(_building_count) == 0
    close_db()


def test_o7_missing_csrf_token_is_rejected_and_leaves_the_site_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o7-csrf.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        client.get("/api/csrf")  # sets the cookie, token deliberately withheld

        resp = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers={"Origin": "http://testserver"},
        )

        assert resp.status_code == 403, resp.text
        assert db.read_sync(_building_count) == 0

        # Paired positive on the SAME client: a request WITH the token
        # succeeds, so the 403 above is attributable to the missing token.
        headers = _csrf_headers(client)
        ok = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )
        assert ok.status_code == 200, ok.text
    close_db()


# ---------------------------------------------------------------------------
# O8 - The loader's applicability agrees with the route's own refusals.
# ---------------------------------------------------------------------------


def test_o8_loader_applicability_agrees_with_the_clean_state() -> None:
    conn = asof_conn()
    site_id = _insert_site(conn)
    conn.commit()

    rows = load_timezone_correction(conn, load_sites(conn))
    row = next(r for r in rows if r.site_id == site_id)

    assert row.applicable is True
    assert row.blocked_reason is None


def test_o8_loader_applicability_agrees_with_a_building_correction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o8-building.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        headers = _csrf_headers(client)
        started = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )
        assert started.status_code == 200, started.text
        generation_id = started.json()["generation_id"]

        row = db.read_sync(
            lambda conn: next(
                r
                for r in load_timezone_correction(conn, load_sites(conn))
                if r.site_id == site_id
            )
        )
        assert row.applicable is False
        assert row.blocked_reason == (
            f"a correction is already building (generation {generation_id})"
        )

        blocked = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": "Etc/GMT+8", "confirm": True},
            headers=headers,
        )
        assert blocked.status_code == 409, blocked.text
    close_db()


def test_o8_loader_applicability_agrees_with_an_active_verification_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o8-chain.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        db.write_sync(lambda conn: _insert_verification_job(conn, site_id))
        headers = _csrf_headers(client)

        row = db.read_sync(
            lambda conn: next(
                r
                for r in load_timezone_correction(conn, load_sites(conn))
                if r.site_id == site_id
            )
        )
        assert row.applicable is False
        assert row.blocked_reason == "a verification run is active for this site"

        blocked = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )
        assert blocked.status_code == 409, blocked.text
    close_db()


# ---------------------------------------------------------------------------
# O9 - load_ops carries the panel key, one row per site, and the shared
# generation_status SELECT runs exactly once (not per-site).
# ---------------------------------------------------------------------------


def test_o9_load_ops_carries_one_row_per_site_with_a_single_shared_select() -> None:
    conn = asof_conn()
    site_a = asof_make_site(conn, "site-a")
    site_b = asof_make_site(conn, "site-b")
    conn.commit()

    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        context = load_ops(conn)
    finally:
        conn.set_trace_callback(None)

    rows = context["timezone_correction"]
    assert isinstance(rows, list)
    row_site_ids = {row.site_id for row in rows}
    assert row_site_ids == {site_a, site_b}

    generation_status_selects = [
        stmt for stmt in statements if "FROM timezone_generations g" in stmt
    ]
    assert len(generation_status_selects) == 1, statements


# ---------------------------------------------------------------------------
# O10 - Successful start creates exactly one generation row and one job row,
# correctly keyed and payloaded.
# ---------------------------------------------------------------------------


def test_o10_successful_start_creates_the_generation_and_job_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o10.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        headers = _csrf_headers(client)

        resp = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )

        assert resp.status_code == 200, resp.text
        generation_id = resp.json()["generation_id"]

        def _check(conn: sqlite3.Connection) -> None:
            gens = conn.execute(
                "SELECT site_id, state, mode, timezone FROM timezone_generations "
                "WHERE id=?",
                (generation_id,),
            ).fetchall()
            assert len(gens) == 1
            assert gens[0]["site_id"] == site_id
            assert gens[0]["state"] == "building"
            assert gens[0]["mode"] == "retrospective_correction"
            assert gens[0]["timezone"] == _NEW_TZ

            jobs = conn.execute(
                "SELECT job_key, payload FROM jobs WHERE type='timezone_correction'"
            ).fetchall()
            assert len(jobs) == 1
            assert jobs[0]["job_key"] == correction_job_key(generation_id)
            import json as _json

            assert _json.loads(jobs[0]["payload"]) == {"generation_id": generation_id}

        db.read_sync(_check)
    close_db()


# ---------------------------------------------------------------------------
# O11 - A site's published history is untouched by a correction request
# until the chain's own flip.
# ---------------------------------------------------------------------------


def test_o11_published_history_is_untouched_by_the_start_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o11.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_seed_correctable_history)

        def _snapshot(conn: sqlite3.Connection) -> tuple[object, ...]:
            site_tz = conn.execute(
                "SELECT timezone FROM sites WHERE id=?", (site_id,)
            ).fetchone()["timezone"]
            published_ptr = published_generation_id(conn, site_id)
            pairs = conn.execute(
                f"SELECT COUNT(*) AS n FROM forecast_pairs fp WHERE fp.site_id=? "
                f"AND {published_generation_clause('fp')}",
                (site_id,),
            ).fetchone()["n"]
            return (site_tz, published_ptr, pairs)

        before = db.read_sync(_snapshot)
        assert before[2] > 0, "fixture must seed at least one published pair"

        headers = _csrf_headers(client)
        resp = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )
        assert resp.status_code == 200, resp.text

        after = db.read_sync(_snapshot)
        assert after == before
    close_db()


# ---------------------------------------------------------------------------
# O12 - A finished correction is observable as finished from domain status.
# ---------------------------------------------------------------------------


def test_o12_finished_correction_is_observable_as_finished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o12.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_seed_correctable_history)
        headers = _csrf_headers(client)
        started = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )
        assert started.status_code == 200, started.text
        generation_id = started.json()["generation_id"]

        db.write_sync(lambda conn: _drive_to_complete(conn, site_id, generation_id))

        def _check(conn: sqlite3.Connection) -> None:
            raw = conn.execute(
                "SELECT state, examined_count, changed_count, unchanged_count, "
                "excluded_count FROM timezone_generations WHERE id=?",
                (generation_id,),
            ).fetchone()
            assert raw is not None
            assert str(raw["state"]) == "published"

            row = next(
                r
                for r in load_timezone_correction(conn, load_sites(conn))
                if r.site_id == site_id
            )
            assert row.building_generation_id is None
            assert row.published_generation_id == generation_id
            assert row.last_published_at is not None
            assert row.failed_generation_id is None
            assert row.applicable is True
            # Counts, derived by reading the seeded fixture's actual write
            # (raw DB row), not hand-computed -- equality with the loader's
            # own read is what this oracle pins, whatever the values are.
            assert row.examined is not None and row.examined > 0
            assert row.examined == raw["examined_count"]
            assert row.changed == raw["changed_count"]
            assert row.unchanged == raw["unchanged_count"]
            assert row.excluded == raw["excluded_count"]

        db.read_sync(_check)
    close_db()


# ---------------------------------------------------------------------------
# O13 - Terminally-failed correction is visible; retry starts cleanly and
# coexists with the still-visible failure notice.
# ---------------------------------------------------------------------------


def test_o13_terminal_failure_is_visible_and_retry_coexists_with_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o13.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_insert_site)
        db.write_sync(lambda conn: _seed_min_history(conn, site_id))
        headers = _csrf_headers(client)

        started = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )
        assert started.status_code == 200, started.text
        failed_generation_id = started.json()["generation_id"]

        def _force_terminal_failure(conn: sqlite3.Connection) -> None:
            job = claim_next_job(conn)
            assert job is not None
            assert job.job_key == correction_job_key(failed_generation_id)
            # Trap #1: force terminal failure through the ROW, never by
            # constructing Job(..., max_retries=0) -- fail() re-reads
            # retry_count/max_retries from the jobs row.
            conn.execute("UPDATE jobs SET max_retries = 0 WHERE id = ?", (job.id,))
            disposition = _fail_job(conn, job, "forced")
            assert disposition is not None and disposition.terminal

        db.write_sync(_force_terminal_failure)

        def _check_failed(conn: sqlite3.Connection) -> None:
            gen = conn.execute(
                "SELECT state FROM timezone_generations WHERE id=?",
                (failed_generation_id,),
            ).fetchone()
            assert gen is not None and str(gen["state"]) == "failed"
            job_row = conn.execute(
                "SELECT status FROM jobs WHERE type='timezone_correction'"
            ).fetchone()
            assert job_row is not None and str(job_row["status"]) == "failed"

            row = next(
                r
                for r in load_timezone_correction(conn, load_sites(conn))
                if r.site_id == site_id
            )
            assert row.failed_generation_id == failed_generation_id
            assert row.building_generation_id is None
            assert row.applicable is True
            assert row.blocked_reason is None

        db.read_sync(_check_failed)

        ops_after_failure = client.get("/ops")
        assert ops_after_failure.status_code == 200, ops_after_failure.text
        failure_html = _norm(ops_after_failure.text)
        assert f"generation {failed_generation_id}" in failure_html
        assert "failed and was abandoned" in failure_html

        # Every page render reissues the CSRF cookie (web/render.py), so the
        # token captured before the GET above is now stale -- refetch it.
        headers = _csrf_headers(client)
        retry = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )
        assert retry.status_code == 200, retry.text
        new_generation_id = retry.json()["generation_id"]
        assert new_generation_id != failed_generation_id

        def _check_coexist(conn: sqlite3.Connection) -> None:
            building = conn.execute(
                "SELECT id FROM timezone_generations WHERE state='building'"
            ).fetchall()
            assert [int(r["id"]) for r in building] == [new_generation_id]
            still_failed = conn.execute(
                "SELECT state FROM timezone_generations WHERE id=?",
                (failed_generation_id,),
            ).fetchone()
            assert str(still_failed["state"]) == "failed"
            still_failed_job = conn.execute(
                "SELECT status FROM jobs WHERE type='timezone_correction' "
                "AND job_key=?",
                (correction_job_key(failed_generation_id),),
            ).fetchone()
            assert str(still_failed_job["status"]) == "failed"
            active_job = conn.execute(
                "SELECT status FROM jobs WHERE type='timezone_correction' "
                "AND job_key=?",
                (correction_job_key(new_generation_id),),
            ).fetchone()
            assert str(active_job["status"]) in ("pending", "running")

            row = next(
                r
                for r in load_timezone_correction(conn, load_sites(conn))
                if r.site_id == site_id
            )
            assert row.failed_generation_id == failed_generation_id
            assert row.building_generation_id == new_generation_id
            assert row.applicable is False
            assert row.blocked_reason == (
                f"a correction is already building (generation {new_generation_id})"
            )

        db.read_sync(_check_coexist)

        ops_after_retry = client.get("/ops")
        assert ops_after_retry.status_code == 200, ops_after_retry.text
        coexist_html = _norm(ops_after_retry.text)
        assert f"generation {failed_generation_id}" in coexist_html
        assert "failed and was abandoned" in coexist_html
        assert f"building (generation {new_generation_id}" in coexist_html
    close_db()


# ---------------------------------------------------------------------------
# O14 - A correction still building is never reported as finished (the
# reconciliation identity holds at 0/0/0/0 too, so it must not be read as
# completion).
# ---------------------------------------------------------------------------


def test_o14_building_correction_is_never_reported_as_finished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o14.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_seed_correctable_history)
        headers = _csrf_headers(client)
        started = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )
        assert started.status_code == 200, started.text
        generation_id = started.json()["generation_id"]

        # Exactly one chunk: the chain-start chunk, which sets up the day
        # range but does not rebuild anything yet -- counts stay 0/0/0/0.
        db.write_sync(lambda conn: _claim_and_step(conn, site_id, generation_id))

        def _check(conn: sqlite3.Connection) -> None:
            raw = conn.execute(
                "SELECT state, examined_count, changed_count, unchanged_count, "
                "excluded_count FROM timezone_generations WHERE id=?",
                (generation_id,),
            ).fetchone()
            assert raw is not None
            assert str(raw["state"]) == "building"
            assert (
                raw["examined_count"],
                raw["changed_count"],
                raw["unchanged_count"],
                raw["excluded_count"],
            ) == (0, 0, 0, 0)

        db.read_sync(_check)

        resp = client.get("/ops")
        assert resp.status_code == 200, resp.text
        html = _norm(resp.text)
        assert f"building (generation {generation_id}" in html
        assert "reconciled" not in html.lower()
        assert f"published generation {generation_id}" not in html
    close_db()


# ---------------------------------------------------------------------------
# O15 - The always-visible disclosure copy is present verbatim (whitespace-
# normalized: the panel's HTML source line-wraps it).
# ---------------------------------------------------------------------------


def test_o15_panel_discloses_stale_inputs_and_verification_fallout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o15.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(_insert_site)

        resp = client.get("/ops")
        assert resp.status_code == 200, resp.text
        html = _norm(resp.text)

        assert "stale inputs" in html
        assert "keep rendering" in html
        assert "will normally fail" in html
    close_db()


# ---------------------------------------------------------------------------
# O16 - A published-but-cleanup-stalled correction is reported as stalled,
# never as a failure. Negative case FIRST: with the cleanup chunk still
# pending, nothing must read as stalled.
# ---------------------------------------------------------------------------


def test_o16_cleanup_stall_is_reported_distinctly_from_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o16.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(_seed_correctable_history)
        headers = _csrf_headers(client)
        started = client.post(
            _POST_PATH.format(site_id=site_id),
            json={"timezone": _NEW_TZ, "confirm": True},
            headers=headers,
        )
        assert started.status_code == 200, started.text
        generation_id = started.json()["generation_id"]

        db.write_sync(
            lambda conn: _drive_to_state(
                conn, site_id, generation_id, stop_state="published"
            )
        )

        # Negative case first: the cleanup-chain job is still pending, so
        # "cleanup still running" must not read as "stalled".
        def _check_negative(conn: sqlite3.Connection) -> None:
            # The cleanup continuation shares its job_key with the completed
            # chunk that enqueued it (build_continuation reuses
            # correction_job_key(generation_id)), so pick the newest row --
            # not just any row matching the key.
            job_row = conn.execute(
                "SELECT status FROM jobs WHERE type='timezone_correction' "
                "AND job_key=? ORDER BY id DESC LIMIT 1",
                (correction_job_key(generation_id),),
            ).fetchone()
            assert job_row is not None
            assert str(job_row["status"]) in ("pending", "running")

            row = next(
                r
                for r in load_timezone_correction(conn, load_sites(conn))
                if r.site_id == site_id
            )
            assert row.cleanup_stalled_generation_id is None

        db.read_sync(_check_negative)

        def _force_terminal_failure(conn: sqlite3.Connection) -> None:
            # The flip enqueued its own pair_and_score side-effect job in the
            # same transaction; drain it first so claim_next_job's global
            # priority-tier ordering (db/queue.py:276-280) doesn't hand back
            # that instead of the cleanup continuation under test.
            _drain_foreign_jobs(conn)
            job = claim_next_job(conn)
            assert job is not None
            assert job.job_key == correction_job_key(generation_id)
            conn.execute("UPDATE jobs SET max_retries = 0 WHERE id = ?", (job.id,))
            disposition = _fail_job(conn, job, "forced")
            assert disposition is not None and disposition.terminal

        db.write_sync(_force_terminal_failure)

        def _check_stalled(conn: sqlite3.Connection) -> None:
            gen = conn.execute(
                "SELECT state FROM timezone_generations WHERE id=?",
                (generation_id,),
            ).fetchone()
            # mark_correction_failed's UPDATE ... WHERE state='building'
            # matches ZERO rows post-flip: the generation stays published.
            assert gen is not None and str(gen["state"]) == "published"
            blob = get_runtime_state(conn, correction_state_key(generation_id))
            assert blob is not None
            import json as _json

            assert _json.loads(blob)["phase"] == "cleanup"

            row = next(
                r
                for r in load_timezone_correction(conn, load_sites(conn))
                if r.site_id == site_id
            )
            assert row.cleanup_stalled_generation_id == generation_id
            assert row.failed_generation_id is None
            assert row.building_generation_id is None
            assert row.applicable is True

        db.read_sync(_check_stalled)

        resp = client.get("/ops")
        assert resp.status_code == 200, resp.text
        html = _norm(resp.text)
        assert f"Generation {generation_id} published" in html
        assert "background cleanup stopped" in html
        assert "failed and was abandoned" not in html
    close_db()


# ---------------------------------------------------------------------------
# O17 - The stalled-cleanup predicate (STALLED_CLEANUP_SQL) is ONE statement
# joining runtime_state and jobs, so an autocommit reader evaluates both
# halves against a single snapshot -- never a runtime_state read followed by
# a separate jobs read that could straddle the worker's two-commit cleanup
# (blob delete, then job completion, in separate transactions; see
# ``STALLED_CLEANUP_SQL``'s own docstring in ``web/context.py``). Plus the
# predicate's full committed-state truth table, direct-seeded rather than
# worker-driven (mirrors O8's direct-loader style): the two states O16
# already exercises via a real chain (blob+no-job, blob+pending-job) are not
# repeated here.
# ---------------------------------------------------------------------------


def _seed_published_correction(
    conn: sqlite3.Connection, site_id: int, *, timezone: str = _NEW_TZ
) -> int:
    """A published retrospective-correction generation, wired via the
    pointer -- the only mode/state ``load_timezone_correction`` ever runs
    the stalled-cleanup check against.
    """
    cur = conn.execute(
        """
        INSERT INTO timezone_generations
            (site_id, timezone, mode, state, published_at)
        VALUES (?, ?, 'retrospective_correction', 'published', ?)
        """,
        (site_id, timezone, "2026-06-11T01:00:00Z"),
    )
    assert cur.lastrowid is not None
    generation_id = int(cur.lastrowid)
    set_runtime_state(conn, published_pointer_key(site_id), str(generation_id))
    return generation_id


def _set_correction_blob(conn: sqlite3.Connection, generation_id: int) -> None:
    import json as _json

    set_runtime_state(
        conn, correction_state_key(generation_id), _json.dumps({"phase": "cleanup"})
    )


def _insert_correction_job(
    conn: sqlite3.Connection, site_id: int, generation_id: int, status: str
) -> None:
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES "
        "('timezone_correction', ?, ?, ?)",
        (site_id, correction_job_key(generation_id), status),
    )


def test_o17_stalled_cleanup_check_issues_a_single_statement() -> None:
    conn = asof_conn()
    site_id = _insert_site(conn)
    generation_id = _seed_published_correction(conn, site_id)
    _set_correction_blob(conn, generation_id)
    conn.commit()

    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        rows = load_timezone_correction(conn, load_sites(conn))
    finally:
        conn.set_trace_callback(None)

    row = next(r for r in rows if r.site_id == site_id)
    assert row.cleanup_stalled_generation_id == generation_id

    # "runtime_state" + "FROM jobs" together, in the SAME statement, is
    # unique to STALLED_CLEANUP_SQL among every query load_timezone_correction
    # runs: generation_status's own runtime_state join never mentions "FROM
    # jobs", and verification_chain_active's ACTIVE_JOB_SQL probe never
    # mentions "runtime_state". A two-statement predicate (a bare
    # get_runtime_state SELECT, then a separate ACTIVE_JOB_SQL execute) would
    # match NEITHER half of this conjunction, so this assertion would read 0
    # -- not 1 -- and fail.
    stall_check_statements = [
        stmt for stmt in statements if "runtime_state" in stmt and "FROM jobs" in stmt
    ]
    assert len(stall_check_statements) == 1, statements


@pytest.mark.parametrize(
    ("blob_present", "job_status", "job_site", "expect_stalled"),
    [
        pytest.param(False, None, "own", False, id="no_blob_no_job"),
        pytest.param(True, "running", "own", False, id="blob_running_job"),
        pytest.param(True, "completed", "own", True, id="blob_completed_job"),
        pytest.param(False, "completed", "own", False, id="no_blob_completed_job"),
        pytest.param(
            True, "running", "other", True, id="blob_job_belongs_to_other_site"
        ),
    ],
)
def test_o17_stalled_cleanup_predicate_truth_table(
    blob_present: bool,
    job_status: str | None,
    job_site: str,
    expect_stalled: bool,
) -> None:
    conn = asof_conn()
    site_id = _insert_site(conn)
    other_site_id = _insert_site(conn, "site-beta")
    generation_id = _seed_published_correction(conn, site_id)
    if blob_present:
        _set_correction_blob(conn, generation_id)
    if job_status is not None:
        owner_site_id = site_id if job_site == "own" else other_site_id
        _insert_correction_job(conn, owner_site_id, generation_id, job_status)
    conn.commit()

    row = next(
        r
        for r in load_timezone_correction(conn, load_sites(conn))
        if r.site_id == site_id
    )
    if expect_stalled:
        assert row.cleanup_stalled_generation_id == generation_id
    else:
        assert row.cleanup_stalled_generation_id is None
