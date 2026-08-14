"""Publish-hold Ops-control-surface oracles (0.11.3, plan §8 O6-O12, O15, O16).

Covers the `PUT /api/verification/publish-hold` handler's refusal/permission
asymmetry (D5), its `MutationGuard` exposure, the one-derivation invariant
between the route and the scheduler's own gate (D10), the startup option
sync's inability to smuggle a publish-hold add-on option (D9), the two
pages' banner rendering including the D14 chain-active copy, and the D12
trigger-decision predicate driving next-tick release semantics.

O16b ("repeated held ticks stay deduplicated") is intentionally NOT
duplicated here: it is already the shape of
``test_publish_hold_is_idempotent_via_the_trigger_decision_early_out`` in
``tests/test_verification_run.py`` (three ticks while held -> one decision
row, no jobs) -- confirmed present and correctly shaped, so authoring it a
second time here would only be a near-duplicate.

Isolation: HTTP-route oracles (O6-O11, O12a-c, O15) drive a real app over
``TestClient`` against a file-backed ``tmp_path`` database, mirroring
``tests/test_ops_feed_health.py``'s pattern (idle worker stand-in,
``close_db``/``config.db_path`` reset per test). O16's four cases drive
``_enqueue_due_verification_runs`` directly against an in-memory connection,
mirroring the existing shape at ``tests/test_verification_run.py:828-847``.

Synthetic data only (public repo): ``site-alpha`` / ``America/Denver``, no
real station or device identifiers.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable
from dataclasses import fields
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.options import RuntimeOptions
from wxverify.db.connection import close_db, get_db
from wxverify.db.migrations import create_schema, run_migrations
from wxverify.db.runtime_state import get_runtime_state, get_runtime_state_entry
from wxverify.settings.keys import get_setting, set_setting
from wxverify.settings.service import apply_plain_settings
from wxverify.verification.publish_hold import (
    PUBLISH_HOLD_BOOTSTRAP_KEY,
    PUBLISH_HOLD_LAST_SOURCE_KEY,
    PUBLISH_HOLD_LAST_STATE_KEY,
    PublishHoldState,
)
from wxverify.worker.scheduler import (
    VERIFICATION_PUBLISH_HOLD_KEY,
    _enqueue_due_verification_runs,
    verification_publish_held,
)
from wxverify.worker.verification_run import verification_job_key

T = TypeVar("T")

_SITE_NAME = "site-alpha"
_SITE_TZ = "America/Denver"
_PUT_PATH = "/api/verification/publish-hold"


# ---------------------------------------------------------------------------
# Shared HTTP-surface helpers.
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
        VALUES (?, 39.7, -104.9, 1600.0, ?, ?)
        """,
        (name, _SITE_TZ, enabled),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_job(conn: sqlite3.Connection, site_id: int, *, status: str) -> None:
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES "
        "('verification_run', ?, ?, ?)",
        (site_id, verification_job_key(site_id), status),
    )


def _seed_armed_site_and_job(conn: sqlite3.Connection, *, job_status: str) -> int:
    """A real site and a real `verification_run` jobs row (`job_key`
    computed, never hardcoded), with the hold armed directly.

    By the time this runs the app's own lifespan has already migrated a
    genuinely fresh database once (fresh-install branch: marker
    `skipped_fresh_install`, hold absent) -- driving the arming through a
    SECOND `run_migrations` call would be a no-op under the correct,
    presence-gated bootstrap (that no-op is exactly O4b's pinned
    invariant), so these route-behavior oracles set the hold with
    `set_setting` directly rather than depending on the bootstrap firing
    twice.
    """
    site_id = _insert_site(conn, enabled=1)
    set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1")
    _insert_job(conn, site_id, status=job_status)
    conn.commit()
    return site_id


# ---------------------------------------------------------------------------
# O6 - Release is refused while a chain is active (paired, non-vacuous).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("job_status", "expect_status", "expect_hold_after"),
    [
        ("running", 409, "1"),
        ("pending", 409, "1"),
        ("completed", 200, "0"),
    ],
)
def test_o6_release_refused_only_while_a_chain_is_actually_active(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    job_status: str,
    expect_status: int,
    expect_hold_after: str,
) -> None:
    app = _boot(tmp_path, f"o6-{job_status}.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(
            lambda conn: _seed_armed_site_and_job(conn, job_status=job_status)
        )
        headers = _csrf_headers(client)

        resp = client.put(
            _PUT_PATH, json={"held": False, "confirm": True}, headers=headers
        )

        assert resp.status_code == expect_status, resp.text
        held = db.read_sync(
            lambda conn: get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY)
        )
        assert held == expect_hold_after
    close_db()


# ---------------------------------------------------------------------------
# O7 - Arming is permitted during an active chain.
# ---------------------------------------------------------------------------


def test_o7_arming_is_permitted_while_a_chain_is_active(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o7.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(lambda conn: _seed_armed_site_and_job(conn, job_status="running"))
        headers = _csrf_headers(client)

        resp = client.put(
            _PUT_PATH, json={"held": True, "confirm": True}, headers=headers
        )

        assert resp.status_code == 200, resp.text
        held = db.read_sync(
            lambda conn: get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY)
        )
        assert held == "1"
    close_db()


# ---------------------------------------------------------------------------
# O8 - Confirmation is required.
# ---------------------------------------------------------------------------


def test_o8_missing_confirm_field_is_rejected_by_the_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o8-missing.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(
            lambda conn: _seed_armed_site_and_job(conn, job_status="completed")
        )
        headers = _csrf_headers(client)

        resp = client.put(_PUT_PATH, json={"held": False}, headers=headers)

        assert resp.status_code == 422, resp.text
        held = db.read_sync(
            lambda conn: get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY)
        )
        assert held == "1"
    close_db()


def test_o8_confirm_false_is_rejected_with_400(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o8-false.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(
            lambda conn: _seed_armed_site_and_job(conn, job_status="completed")
        )
        headers = _csrf_headers(client)

        resp = client.put(
            _PUT_PATH, json={"held": False, "confirm": False}, headers=headers
        )

        assert resp.status_code == 400, resp.text
        held = db.read_sync(
            lambda conn: get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY)
        )
        assert held == "1"
    close_db()


# ---------------------------------------------------------------------------
# O9 - The route sits behind MutationGuard.
# ---------------------------------------------------------------------------


def test_o9_missing_csrf_token_is_rejected_and_leaves_the_hold_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o9-csrf.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(
            lambda conn: _seed_armed_site_and_job(conn, job_status="completed")
        )
        client.get("/api/csrf")  # sets the cookie, token deliberately withheld

        resp = client.put(
            _PUT_PATH,
            json={"held": False, "confirm": True},
            headers={"Origin": "http://testserver"},
        )

        assert resp.status_code == 403, resp.text
        held = db.read_sync(
            lambda conn: get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY)
        )
        assert held == "1"

        # Paired positive on the SAME client: a request WITH the token
        # succeeds, so the 403 above is attributable to the missing token
        # and not to some other guard condition.
        headers = _csrf_headers(client)
        ok = client.put(
            _PUT_PATH, json={"held": False, "confirm": True}, headers=headers
        )
        assert ok.status_code == 200, ok.text
    close_db()


def test_o9_disallowed_content_type_is_rejected_and_leaves_the_hold_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o9-ctype.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(
            lambda conn: _seed_armed_site_and_job(conn, job_status="completed")
        )
        token = client.get("/api/csrf").json()["csrf_token"]

        resp = client.put(
            _PUT_PATH,
            content=b'{"held": false, "confirm": true}',
            headers={
                "Origin": "http://testserver",
                "X-CSRF-Token": token,
                "Content-Type": "text/plain",
            },
        )

        assert resp.status_code == 415, resp.text
        held = db.read_sync(
            lambda conn: get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY)
        )
        assert held == "1"
    close_db()


# ---------------------------------------------------------------------------
# O10 - One derivation: the route and the scheduler's own gate agree.
# ---------------------------------------------------------------------------


def test_o10_route_write_and_the_schedulers_own_gate_agree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o10.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(
            lambda conn: _seed_armed_site_and_job(conn, job_status="completed")
        )
        headers = _csrf_headers(client)

        release_resp = client.put(
            _PUT_PATH, json={"held": False, "confirm": True}, headers=headers
        )
        assert release_resp.status_code == 200, release_resp.text
        assert release_resp.json()["held"] is False
        assert db.read_sync(lambda conn: verification_publish_held(conn)) is False, (
            "scheduler.verification_publish_held disagrees with the route's response"
        )

        arm_resp = client.put(
            _PUT_PATH, json={"held": True, "confirm": True}, headers=headers
        )
        assert arm_resp.status_code == 200, arm_resp.text
        assert arm_resp.json()["held"] is True
        assert db.read_sync(lambda conn: verification_publish_held(conn)) is True, (
            "scheduler.verification_publish_held disagrees with the route's response"
        )
    close_db()


def test_o10_companion_response_body_is_the_state_dataclass_verbatim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The read-back is `PublishHoldState` serialized field-for-field.

    The Ops control renders this payload straight into the panel by field
    name (``tests/test_app_js_publish_hold.py`` pins the JavaScript half),
    so an alias layer, a renamed field, or a stringified boolean here would
    blank or misreport a control that says whether publishing is held.
    """
    app = _boot(tmp_path, "o10-shape.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(
            lambda conn: _seed_armed_site_and_job(conn, job_status="completed")
        )
        headers = _csrf_headers(client)

        resp = client.put(
            _PUT_PATH, json={"held": False, "confirm": True}, headers=headers
        )

        assert resp.status_code == 200, resp.text
        payload = resp.json()
        assert set(payload) == {f.name for f in fields(PublishHoldState)}
        assert payload["held"] is False
        assert payload["chain_active"] is False
        assert payload["last_state"] == "released"
        assert payload["last_source"] == "ops"
    close_db()


# ---------------------------------------------------------------------------
# O11 - Startup option sync cannot overwrite the hold.
# ---------------------------------------------------------------------------


def test_o11_apply_plain_settings_does_not_touch_the_publish_hold(
    tmp_path: Path,
) -> None:
    close_db()
    config.db_path = str(tmp_path / "o11.db")
    config.options_path = str(tmp_path / "missing-options.json")
    db = get_db()
    db.write_sync(lambda conn: set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1"))

    asyncio.run(apply_plain_settings(RuntimeOptions()))

    held = db.read_sync(lambda conn: get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY))
    assert held == "1"
    close_db()


def test_o11_runtime_options_declares_no_publish_hold_field() -> None:
    assert not any("publish_hold" in name for name in RuntimeOptions.model_fields), (
        "a publish-hold add-on option field is exactly the design mistake D9 forbids"
    )


# ---------------------------------------------------------------------------
# O12a - Banner rendering: held vs released, both pages.
# ---------------------------------------------------------------------------

_HELD_BANNER_TEXT = "Verification publishing is held."
_NO_SITES_TEXT = "No sites configured."


@pytest.mark.parametrize("path", ["/ops", "/verification"])
def test_o12a_banner_present_when_held_absent_when_released(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    app = _boot(tmp_path, f"o12a-{path.strip('/')}.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(lambda conn: _insert_site(conn, enabled=1))
        db.write_sync(
            lambda conn: set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1")
        )

        held_resp = client.get(path)
        assert held_resp.status_code == 200
        assert _HELD_BANNER_TEXT in held_resp.text

        db.write_sync(
            lambda conn: set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "0")
        )
        released_resp = client.get(path)
        assert released_resp.status_code == 200
        assert _HELD_BANNER_TEXT not in released_resp.text
    close_db()


# ---------------------------------------------------------------------------
# O12b - /verification renders, and shows the banner, with no resolvable
# site. Pins the placement decision: `publish_hold` is built above
# load_verification's early return.
# ---------------------------------------------------------------------------


def test_o12b_i_no_sites_at_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o12b-i.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        # Fresh branch (no enabled site) -- the bootstrap would leave the
        # hold absent, so this fixture must set it explicitly.
        db.write_sync(
            lambda conn: set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1")
        )

        resp = client.get("/verification")

        assert resp.status_code == 200
        assert _HELD_BANNER_TEXT in resp.text
        assert _NO_SITES_TEXT in resp.text
    close_db()


def test_o12b_ii_one_site_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o12b-ii.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(lambda conn: _insert_site(conn, enabled=0))
        db.write_sync(
            lambda conn: set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1")
        )

        resp = client.get("/verification")

        assert resp.status_code == 200
        assert _HELD_BANNER_TEXT in resp.text
        assert _NO_SITES_TEXT in resp.text
    close_db()


def test_o12b_iii_unknown_site_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o12b-iii.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(lambda conn: _insert_site(conn, enabled=1))
        db.write_sync(
            lambda conn: set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1")
        )

        resp = client.get("/verification", params={"site": 999999})

        assert resp.status_code == 200
        assert _HELD_BANNER_TEXT in resp.text
    close_db()


def test_o12b_companion_ops_page_no_sites_still_renders_the_control_panel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Operator-requested companion: /ops must render both the banner and
    the ops publish-hold control panel with zero sites (unlike
    `/verification`, `load_ops` includes disabled sites too, so a
    disabled-only fixture is not a distinguishing companion here -- the
    empty-database case is the one that would expose a hidden site-count
    dependency in `load_ops`/`ops/show.html`)."""
    app = _boot(tmp_path, "o12b-ops.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(
            lambda conn: set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1")
        )

        resp = client.get("/ops")

        assert resp.status_code == 200
        assert _HELD_BANNER_TEXT in resp.text
        assert 'id="publish-hold-state"' in resp.text

        db.write_sync(
            lambda conn: set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "0")
        )
        released = client.get("/ops")
        assert released.status_code == 200
        assert _HELD_BANNER_TEXT not in released.text
        assert 'id="publish-hold-state"' in released.text, (
            "the ops control panel itself must render regardless of hold "
            "state -- only the banner is conditional"
        )
    close_db()


# ---------------------------------------------------------------------------
# O12c - The banner tells the truth during an active chain (D14).
# ---------------------------------------------------------------------------

_CHAIN_ACTIVE_TEXT = "a verification run is already in progress"
_NO_CHAIN_TEXT = "New verification runs are not being started."
_OVER_PROMISE_TEXT = "Nothing publishes"


def test_o12c_banner_reflects_chain_active_state_truthfully(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o12c-no-chain.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(lambda conn: _insert_site(conn, enabled=1))
        db.write_sync(
            lambda conn: set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1")
        )

        no_chain_resp = client.get("/verification")
        assert no_chain_resp.status_code == 200
        assert _NO_CHAIN_TEXT in no_chain_resp.text
        assert _CHAIN_ACTIVE_TEXT not in no_chain_resp.text
        assert _OVER_PROMISE_TEXT not in no_chain_resp.text
    close_db()


def test_o12c_banner_reflects_chain_active_state_truthfully_when_active(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o12c-chain.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            site_id = _insert_site(conn, enabled=1)
            _insert_job(conn, site_id, status="running")

        db.write_sync(_seed)
        db.write_sync(
            lambda conn: set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1")
        )

        active_resp = client.get("/verification")
        assert active_resp.status_code == 200
        assert _CHAIN_ACTIVE_TEXT in active_resp.text
        assert _NO_CHAIN_TEXT not in active_resp.text
        assert _OVER_PROMISE_TEXT not in active_resp.text
    close_db()


# ---------------------------------------------------------------------------
# O15 - The HTTP release preserves the bootstrap marker.
# ---------------------------------------------------------------------------


def test_o15_http_release_preserves_the_bootstrap_marker_exactly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The database file must already be in the emulated pre-0.11.3 shape
    # BEFORE the app's own lifespan runs its first `run_migrations` --
    # otherwise that first boot takes the fresh-install branch (no schema,
    # no enabled site yet) and sets the marker to `skipped_fresh_install`,
    # which presence-gates every later `run_migrations` call and makes the
    # arming this oracle depends on unreachable (O4b's own invariant).
    db_path = tmp_path / "o15.db"
    donor = sqlite3.connect(str(db_path))
    donor.row_factory = sqlite3.Row
    donor.execute("PRAGMA foreign_keys=ON")
    create_schema(donor)
    donor.execute("PRAGMA user_version = 4")
    _insert_site(donor, enabled=1)
    donor.commit()
    donor.close()

    app = _boot(tmp_path, "o15.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()
        assert (
            db.read_sync(lambda conn: get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY))
            == "1"
        ), "fixture precondition: the app's own first-boot migration must arm the hold"
        headers = _csrf_headers(client)

        resp = client.put(
            _PUT_PATH, json={"held": False, "confirm": True}, headers=headers
        )
        assert resp.status_code == 200, resp.text

        def _read(conn: sqlite3.Connection) -> dict[str, str | None]:
            return {
                "hold": get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY),
                "marker": get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY),
                "last_state": get_runtime_state(conn, PUBLISH_HOLD_LAST_STATE_KEY),
                "last_source": get_runtime_state(conn, PUBLISH_HOLD_LAST_SOURCE_KEY),
            }

        state = db.read_sync(_read)
        assert state["hold"] == "0"
        assert state["marker"] == "armed"
        assert state["last_state"] == "released"
        assert state["last_source"] == "ops"

        entry = db.read_sync(
            lambda conn: get_runtime_state_entry(conn, PUBLISH_HOLD_LAST_STATE_KEY)
        )
        assert entry is not None
        assert entry.updated_at  # non-empty
        datetime.fromisoformat(entry.updated_at.replace("Z", "+00:00"))

        key_set = db.read_sync(
            lambda conn: {
                str(r["key"])
                for r in conn.execute(
                    "SELECT key FROM runtime_state "
                    "WHERE key LIKE 'verification_publish_hold%'"
                ).fetchall()
            }
        )
        assert key_set == {
            PUBLISH_HOLD_BOOTSTRAP_KEY,
            PUBLISH_HOLD_LAST_STATE_KEY,
            PUBLISH_HOLD_LAST_SOURCE_KEY,
        }
    close_db()

    # Restart: construct a second Database on the same file.
    close_db()
    reopened = get_db()
    still_released = reopened.read_sync(
        lambda conn: get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY)
    )
    assert still_released == "0"
    close_db()


# ---------------------------------------------------------------------------
# O16 - Releasing the hold takes effect on the next tick, same local day.
# ---------------------------------------------------------------------------


class _RealDb:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    async def write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return fn(self._conn)


def _make_verification_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _o16_site(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites
            (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
        VALUES (?, 39.7, -104.9, 1600.0, ?, 1)
        """,
        (_SITE_NAME, _SITE_TZ),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _decision_rows(
    conn: sqlite3.Connection, site_id: int
) -> list[tuple[str, str, str]]:
    return [
        (
            str(r["trigger_date"]),
            str(r["decision"]),
            "" if r["reason"] is None else str(r["reason"]),
        )
        for r in conn.execute(
            "SELECT trigger_date, decision, reason FROM verification_trigger_decisions "
            "WHERE site_id = ? ORDER BY id",
            (site_id,),
        ).fetchall()
    ]


def _job_count(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE type = 'verification_run'"
        ).fetchone()["n"]
    )


def test_o16a_same_day_release_enqueues_and_preserves_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _make_verification_conn()
    site_id = _o16_site(conn)
    tick_time = datetime(2026, 6, 6, 12, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.scheduler.utc_now", lambda: tick_time)
    set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1")

    _enqueue_due_verification_runs(conn)
    assert _job_count(conn) == 0
    assert _decision_rows(conn, site_id) == [("2026-06-06", "skipped", "publish_hold")]
    first_row = conn.execute(
        "SELECT id, decided_at FROM verification_trigger_decisions WHERE site_id = ?",
        (site_id,),
    ).fetchone()

    set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "0")
    _enqueue_due_verification_runs(conn)

    assert _job_count(conn) == 1
    rows = conn.execute(
        "SELECT id, decided_at FROM verification_trigger_decisions WHERE site_id = ?",
        (site_id,),
    ).fetchall()
    assert len(rows) == 1, "the hold-skip row must survive release, not be deleted"
    assert int(rows[0]["id"]) == int(first_row["id"])
    assert str(rows[0]["decided_at"]) == str(first_row["decided_at"])


@pytest.mark.parametrize(
    ("decision", "reason", "expect_jobs_after_release"),
    [
        ("no_change_skip", "no change", 0),
        ("run_started", None, 0),
        ("suppressed_because_active", "verification chain already active", 0),
        ("skipped", "no change", 0),  # load-bearing: a non-hold `skipped` row
    ],
)
def test_o16c_a_prior_non_hold_decision_stays_terminal(
    monkeypatch: pytest.MonkeyPatch,
    decision: str,
    reason: str | None,
    expect_jobs_after_release: int,
) -> None:
    conn = _make_verification_conn()
    site_id = _o16_site(conn)
    tick_time = datetime(2026, 6, 6, 12, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.scheduler.utc_now", lambda: tick_time)
    set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1")
    _enqueue_due_verification_runs(conn)
    assert _job_count(conn) == 0

    conn.execute(
        "INSERT INTO verification_trigger_decisions "
        "(site_id, trigger_date, decision, reason, decided_at) "
        "VALUES (?, '2026-06-06', ?, ?, '2026-06-06T12:00:01Z')",
        (site_id, decision, reason),
    )

    set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "0")
    _enqueue_due_verification_runs(conn)

    assert _job_count(conn) == expect_jobs_after_release
    rows = _decision_rows(conn, site_id)
    assert ("2026-06-06", "skipped", "publish_hold") in rows
    assert ("2026-06-06", decision, "" if reason is None else reason) in rows
    assert len(rows) == 2, "both rows must survive; no redeciding of a terminal date"


def test_o16d_a_null_reason_skip_also_stays_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _make_verification_conn()
    site_id = _o16_site(conn)
    tick_time = datetime(2026, 6, 6, 12, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.scheduler.utc_now", lambda: tick_time)
    set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1")
    _enqueue_due_verification_runs(conn)
    assert _job_count(conn) == 0

    conn.execute(
        "INSERT INTO verification_trigger_decisions "
        "(site_id, trigger_date, decision, reason, decided_at) "
        "VALUES (?, '2026-06-06', 'skipped', NULL, '2026-06-06T12:00:01Z')",
        (site_id,),
    )

    set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "0")
    _enqueue_due_verification_runs(conn)

    assert _job_count(conn) == 0
    rows = _decision_rows(conn, site_id)
    assert ("2026-06-06", "skipped", "publish_hold") in rows
    assert ("2026-06-06", "skipped", "") in rows
    assert len(rows) == 2
