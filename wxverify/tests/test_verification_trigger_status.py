"""W9 (§12) nightly trigger-decision status, on BOTH reader surfaces.

`GET /api/verification/status` and the `/verification` page derive their
trigger status independently of each other's code paths but from one
helper, so every state here is asserted on both and a parity oracle pins
that they agree. The §3.1 publish-hold surfacing rides on the same payload
and is covered here too.

Synthetic fixtures only: invented site names, `America/Denver` /
`Etc/GMT+7` timezones, no real station identifiers.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import asof_conn, asof_make_site
from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import init_db
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.settings.keys import set_setting
from wxverify.verification.runs import (
    PUBLISH_HOLD_REASON,
    expected_trigger_date,
    latest_trigger_decision,
    record_trigger_decision,
    trigger_status,
)
from wxverify.worker import scheduler as scheduler_module
from wxverify.worker.scheduler import _enqueue_due_verification_runs  # noqa: SLF001
from wxverify.worker.verification_run import SUPERSEDED_REASON

# `America/Denver` is UTC-6 in June, so the 05:00 local trigger instant for
# local day D is D 11:00Z.
_TZ = "America/Denver"
#: 06:00 local on 2026-06-06 — past that day's 05:00 trigger.
_AFTER_TRIGGER = datetime(2026, 6, 6, 12, 0, tzinfo=UTC)
#: 01:00 local on 2026-06-06 — before that day's 05:00 trigger, so the
#: reader's current cycle is still 2026-06-05's.
_BEFORE_TRIGGER = datetime(2026, 6, 6, 7, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_phase7_surface.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    from wxverify.db.connection import close_db

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


def _make_site(
    conn: sqlite3.Connection, name: str, timezone: str = _TZ, *, enabled: int = 1
) -> int:
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 40.0, -105.0, 1600.0, ?, ?)
            """,
            (name, timezone, enabled),
        ).lastrowid
    )
    ensure_published_generation(conn, site_id)
    conn.commit()
    return site_id


def _freeze(monkeypatch: pytest.MonkeyPatch, now: datetime) -> None:
    """Pin the clock on BOTH reader surfaces to the same instant."""
    monkeypatch.setattr("wxverify.api.routes.verification.utc_now", lambda: now)
    monkeypatch.setattr("wxverify.web.verification.utc_now", lambda: now)


def _seed(
    conn: sqlite3.Connection,
    site_id: int,
    trigger_date: str,
    decision: str,
    reason: str | None,
) -> None:
    record_trigger_decision(
        conn,
        site_id,
        trigger_date=trigger_date,
        decision=decision,
        reason=reason,
    )
    conn.commit()


def _api_trigger(client: TestClient, site_id: int) -> dict[str, Any]:
    payload = client.get(f"/api/verification/status?site={site_id}").json()
    (entry,) = payload["sites"]
    trigger: dict[str, Any] = entry["trigger"]
    return trigger


def _page(client: TestClient, site_id: int) -> str:
    response = client.get(f"/verification?site={site_id}")
    assert response.status_code == 200
    return response.text


def _page_status(html: str) -> str:
    marker = 'data-trigger-status="'
    assert marker in html, "the page renders no trigger-status element"
    start = html.index(marker) + len(marker)
    return html[start : html.index('"', start)]


# ---------------------------------------------------------------------------
# The six states, each on both surfaces.
# ---------------------------------------------------------------------------


def test_before_the_local_trigger_time_reads_the_previous_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Kills a reader keyed on the site-local *today*: at 01:00 local the
    current cycle is still the previous local date's, and tonight's row does
    not exist yet."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "site-alpha")
    _seed(conn, site_id, "2026-06-05", "run_started", None)
    _seed(conn, site_id, "2026-06-06", "skipped", "inputs were not ready")
    _freeze(monkeypatch, _BEFORE_TRIGGER)

    with TestClient(_make_app(monkeypatch)) as client:
        trigger = _api_trigger(client, site_id)
        html = _page(client, site_id)

    assert trigger["trigger_date"] == "2026-06-05"
    assert trigger["status"] == "triggered"
    assert _page_status(html) == "triggered"
    assert "2026-06-05" in html
    # The later row belongs to a cycle that has not begun for this reader.
    assert "inputs were not ready" not in html


def test_after_the_local_trigger_time_reports_the_skip_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "site-alpha")
    _seed(conn, site_id, "2026-06-06", "skipped", "no settled truth")
    _freeze(monkeypatch, _AFTER_TRIGGER)

    with TestClient(_make_app(monkeypatch)) as client:
        trigger = _api_trigger(client, site_id)
        html = _page(client, site_id)

    assert trigger["trigger_date"] == "2026-06-06"
    assert trigger["status"] == "skipped"
    assert trigger["reason"] == "no settled truth"
    assert _page_status(html) == "skipped"
    assert "no settled truth" in html


def test_a_stale_row_from_an_earlier_date_is_not_reported_as_tonight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Kills a site-scoped ``latest`` read: an old skip row must not be
    rendered as the current cycle's decision."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "site-alpha")
    _seed(conn, site_id, "2026-06-01", "skipped", "attempt cap reached")
    _freeze(monkeypatch, _AFTER_TRIGGER)

    with TestClient(_make_app(monkeypatch)) as client:
        trigger = _api_trigger(client, site_id)
        html = _page(client, site_id)

    assert trigger["status"] == "no_decision_recorded"
    assert trigger["trigger_date"] == "2026-06-06"
    assert trigger["reason"] is None
    assert _page_status(html) == "no_decision_recorded"
    assert "attempt cap reached" not in html


def test_no_row_at_all_is_its_own_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "site-alpha")
    _freeze(monkeypatch, _AFTER_TRIGGER)

    with TestClient(_make_app(monkeypatch)) as client:
        trigger = _api_trigger(client, site_id)
        html = _page(client, site_id)

    assert trigger["status"] == "no_decision_recorded"
    assert _page_status(html) == "no_decision_recorded"


def test_the_w8_three_row_sequence_reports_triggered_and_surfaces_the_supersede(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sequence the W8 chain actually emits — ``run_started``,
    superseding ``skipped``, ``run_started``.

    Kills a bare highest-``id`` reader: the newest row is a clean
    ``run_started``, so without the supersede tally a divergent night
    renders byte-identically to a clean one. Also pins that the rendered
    supersede text comes from ``reason`` and not from ``decision`` — the
    enum is shared with an ordinary gate skip.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "site-alpha")
    _seed(conn, site_id, "2026-06-06", "run_started", None)
    _seed(conn, site_id, "2026-06-06", "skipped", SUPERSEDED_REASON)
    _seed(conn, site_id, "2026-06-06", "run_started", None)
    _freeze(monkeypatch, _AFTER_TRIGGER)

    with TestClient(_make_app(monkeypatch)) as client:
        trigger = _api_trigger(client, site_id)
        html = _page(client, site_id)

    assert trigger["status"] == "triggered"
    assert trigger["superseded_count"] == 1
    assert trigger["superseded_reason"] == SUPERSEDED_REASON
    assert _page_status(html) == "triggered"
    assert SUPERSEDED_REASON in html


def test_a_corrupt_timezone_is_contained_to_its_own_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The containment is the assertion: one unusable timezone must not 500
    the payload or blank the page for the healthy sites beside it."""
    conn = _init_tmp_db(tmp_path)
    healthy = _make_site(conn, "site-alpha")
    broken = _make_site(conn, "site-beta", timezone="Not/AZone")
    _seed(conn, healthy, "2026-06-06", "run_started", None)
    _freeze(monkeypatch, _AFTER_TRIGGER)

    with TestClient(_make_app(monkeypatch)) as client:
        allsites = client.get("/api/verification/status").json()["sites"]
        broken_html = _page(client, broken)
        healthy_html = _page(client, healthy)

    by_site = {int(entry["site_id"]): entry["trigger"] for entry in allsites}
    assert by_site[broken]["status"] == "trigger_date_unknown"
    assert by_site[broken]["trigger_date"] is None
    assert by_site[healthy]["status"] == "triggered"
    assert by_site[healthy]["trigger_date"] == "2026-06-06"
    assert _page_status(broken_html) == "trigger_date_unknown"
    assert _page_status(healthy_html) == "triggered"


# ---------------------------------------------------------------------------
# Parity, and the read path's purity.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("decision", "reason"),
    [
        ("run_started", None),
        ("skipped", "publish_hold"),
        ("no_change_skip", "input fingerprint matches the published run"),
        ("suppressed_because_active", "verification chain already active"),
    ],
)
def test_api_and_page_report_the_identical_trigger_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    decision: str,
    reason: str | None,
) -> None:
    """Record-page parity in the shape W3 uses: every field the payload
    carries is present, with the same value, in the rendered page."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "site-alpha")
    _seed(conn, site_id, "2026-06-06", decision, reason)
    _freeze(monkeypatch, _AFTER_TRIGGER)

    with TestClient(_make_app(monkeypatch)) as client:
        trigger = _api_trigger(client, site_id)
        html = _page(client, site_id)

    assert _page_status(html) == trigger["status"]
    assert str(trigger["trigger_date"]) in html
    if reason is not None:
        assert reason in html


def test_the_trigger_read_path_opens_no_write_transaction() -> None:
    """Engages SQLite's own write guard: any INSERT the read attempts raises
    rather than merely failing an assertion about writes."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "site-alpha")
    conn.execute("UPDATE sites SET timezone = ? WHERE id = ?", (_TZ, site_id))
    record_trigger_decision(
        conn,
        site_id,
        trigger_date="2026-06-06",
        decision="skipped",
        reason="no settled truth",
    )
    conn.commit()
    conn.execute("PRAGMA query_only=ON")

    status = trigger_status(conn, site_id, _AFTER_TRIGGER)

    assert status["status"] == "skipped"
    assert status["reason"] == "no settled truth"
    assert not conn.in_transaction


def test_expected_trigger_date_returns_none_for_an_unresolvable_site() -> None:
    """The failure policy stated with the signature: None, never a raise."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "site-beta")
    conn.execute("UPDATE sites SET timezone = 'Not/AZone' WHERE id = ?", (site_id,))

    assert expected_trigger_date(conn, site_id, _AFTER_TRIGGER) is None
    assert expected_trigger_date(conn, 999_999, _AFTER_TRIGGER) is None


def test_latest_trigger_decision_is_scoped_to_the_exact_date() -> None:
    conn = asof_conn()
    site_id = asof_make_site(conn, "site-alpha")
    for decision, reason in (
        ("run_started", None),
        ("skipped", SUPERSEDED_REASON),
        ("run_started", None),
    ):
        record_trigger_decision(
            conn,
            site_id,
            trigger_date="2026-06-06",
            decision=decision,
            reason=reason,
        )
    record_trigger_decision(
        conn,
        site_id,
        trigger_date="2026-06-07",
        decision="skipped",
        reason="attempt cap reached",
    )

    read = latest_trigger_decision(conn, site_id, "2026-06-06")

    assert read is not None
    assert read.decision == "run_started"
    assert read.superseded_count == 1
    assert read.superseded_reason == SUPERSEDED_REASON
    assert latest_trigger_decision(conn, site_id, "2026-06-01") is None


# ---------------------------------------------------------------------------
# §3.1 — the publish hold on the same payload.
# ---------------------------------------------------------------------------


def test_the_publish_hold_and_its_reason_ride_on_both_surfaces(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "site-alpha")
    set_setting(conn, "verification_publish_hold", "1")
    conn.commit()
    _freeze(monkeypatch, _AFTER_TRIGGER)

    with TestClient(_make_app(monkeypatch)) as client:
        trigger = _api_trigger(client, site_id)
        html = _page(client, site_id)

    assert trigger["publish_hold"] == {"held": True, "reason": PUBLISH_HOLD_REASON}
    assert "Publishing is held by the operator" in html


def test_without_the_hold_key_neither_surface_claims_a_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Negative control: without it the first test would pass against a
    surface that always reports held."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "site-alpha")
    _freeze(monkeypatch, _AFTER_TRIGGER)

    with TestClient(_make_app(monkeypatch)) as client:
        trigger = _api_trigger(client, site_id)
        html = _page(client, site_id)

    assert trigger["publish_hold"] == {"held": False, "reason": None}
    assert "Publishing is held by the operator" not in html


def test_the_hold_row_and_the_read_side_carry_the_one_shared_constant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``PUBLISH_HOLD_REASON`` is the single definition, imported by both
    sides, so this is an identity assertion rather than a spelling pin: it
    fails if the scheduler ever writes some *other* reason on the hold path,
    and cannot be made to pass by editing two literals in step.

    Kills: the 0.11.0-era arrangement this replaced, in which the writer and
    the read side each carried their own ``"publish_hold"`` literal. Any
    divergence between them — the scheduler stamping a different reason on
    the hold path, or the read side reporting one — fails here. Note the
    honest limit: two literals that agree are behaviourally equivalent to
    one shared constant, so this oracle pins the *agreement*, not the
    de-duplication; the single definition is what removes the drift, and
    only a drifted pair is observable from outside."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "site-alpha")
    set_setting(conn, "verification_publish_hold", "1")
    monkeypatch.setattr(scheduler_module, "utc_now", lambda: _AFTER_TRIGGER)

    _enqueue_due_verification_runs(conn)

    row = conn.execute(
        "SELECT reason FROM verification_trigger_decisions WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    assert str(row["reason"]) == PUBLISH_HOLD_REASON
    assert trigger_status(conn, site_id, _AFTER_TRIGGER)["reason"] == (
        PUBLISH_HOLD_REASON
    )


# ---------------------------------------------------------------------------
# The writer side keeps its own, fail-closed rule.
# ---------------------------------------------------------------------------


def test_the_scheduler_still_gates_on_its_own_trigger_instant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reader resolves to the PREVIOUS local date before the trigger
    time; the scheduler must never enqueue for it. One tick before, one
    after, on the same fixture."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "site-alpha")

    monkeypatch.setattr(scheduler_module, "utc_now", lambda: _BEFORE_TRIGGER)
    _enqueue_due_verification_runs(conn)
    assert expected_trigger_date(conn, site_id, _BEFORE_TRIGGER) == "2026-06-05"
    jobs = conn.execute(
        "SELECT payload FROM jobs WHERE type = 'verification_run' AND site_id = ?",
        (site_id,),
    ).fetchall()
    assert jobs == []

    monkeypatch.setattr(scheduler_module, "utc_now", lambda: _AFTER_TRIGGER)
    _enqueue_due_verification_runs(conn)
    jobs = conn.execute(
        "SELECT payload FROM jobs WHERE type = 'verification_run' AND site_id = ?",
        (site_id,),
    ).fetchall()
    assert len(jobs) == 1
    assert "2026-06-06" in str(jobs[0]["payload"])
