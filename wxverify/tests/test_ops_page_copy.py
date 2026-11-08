"""Item 8 (§1.2/§6.14): the Ops page carries controls, not documentation.

A scripted deletion removed explanatory prose from the publish-hold panel
(`ops/_publish_hold.html`); the publish-hold heading was renamed from
"Nightly Verification Publishing" to "Nightly Verification Run Hold"; and
the one fact the deleted prose carried -- that a run already in progress
continues and may publish while the hold is armed -- was moved into the
arm confirmation dialog in `app.js`.

Isolation: HTTP-route oracles drive a real app over `TestClient` against a
file-backed `tmp_path` database with an idle worker stand-in.

Synthetic data only (public repo): no real station or device identifiers.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db
from wxverify.db.runtime_state import set_runtime_state
from wxverify.web import context as context_module
from wxverify.worker.access_hold import access_hold_key

ROOT = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# Harness.
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


def _norm(text: str) -> str:
    """Collapse the template's line-wrapped whitespace for substring checks."""
    return " ".join(text.split())


# ---------------------------------------------------------------------------
# MS1 (plan §13.8, 0.16.13): the Key Status panel's `access_holds` lines
# have NO 24h "still listed" cutoff (unlike the monitor alert's AL1-AL8
# above) -- a hold whose `until` has long passed still renders, its tail
# switching from "held until ..." to "waiting for the next try" purely on
# whether that `until` is still in the future of `/ops`'s own render-time
# clock. Both branches are driven in one test by monkeypatching
# `wxverify.web.context.utc_now` to two different fixed instants around the
# same seeded `until`, over a real HTTP GET and real DB rows -- never a
# template-source read.
# ---------------------------------------------------------------------------


def test_ms1_access_hold_line_renders_with_no_24h_cutoff_and_flips_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "ms1.db", monkeypatch)
    # `since` is deliberately 5+ days before `held_now` below -- far more
    # than the monitor alert's 24h cutoff -- to prove the Ops line has none.
    since_text = "2025-12-30T00:00:00Z"
    until_text = "2026-01-08T00:00:00Z"
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: object) -> None:
            set_runtime_state(
                conn,  # type: ignore[arg-type]
                access_hold_key("hourly"),
                json.dumps({"since": since_text, "until": until_text, "status": 401}),
            )

        db.write_sync(_seed)

        # Branch 1: render-time clock is before `until` (far more than 24h
        # after `since`, which the monitor alert's AL4 would have aged out --
        # the Ops line must still show it, unlike the alert).
        held_now = datetime(2026, 1, 5, 0, 0, 0, tzinfo=UTC)
        monkeypatch.setattr(context_module, "utc_now", lambda: held_now)
        resp = client.get("/ops")
        assert resp.status_code == 200, resp.text
        html = _norm(resp.text)
        label = "hourly readings"
        assert (
            f"weather.com access refused (HTTP 401): {label}, since {since_text}, "
            f"held until {until_text}" in html
        )
        assert "throttled" not in html.lower()
        assert "bad key" not in html.lower()

        # Branch 2: render-time clock is after `until` -- same seeded row,
        # tail flips to "waiting for the next try".
        waiting_now = datetime(2026, 1, 10, 0, 0, 0, tzinfo=UTC)
        monkeypatch.setattr(context_module, "utc_now", lambda: waiting_now)
        resp = client.get("/ops")
        assert resp.status_code == 200, resp.text
        html = _norm(resp.text)
        assert (
            f"weather.com access refused (HTTP 401): {label}, since {since_text}, "
            "waiting for the next try" in html
        )
        assert f"held until {until_text}" not in html
        assert "throttled" not in html.lower()
        assert "bad key" not in html.lower()
    close_db()


def test_ms1_unreadable_hold_renders_without_times_and_never_as_throttled_or_bad_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A hold whose stored value is not a JSON object renders the
    "(unreadable hold), waiting for the next try" line with no HTTP-status
    head and no "since"/"held until" clause -- and the panel never falls
    back to describing it as a "throttled" or "bad key" condition (the
    wording those other budget conditions use), which would be the wrong
    diagnosis for an access-refused hold.
    """
    app = _boot(tmp_path, "ms1-unreadable.db", monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: object) -> None:
            set_runtime_state(
                conn,  # type: ignore[arg-type]
                access_hold_key("five_minute"),
                json.dumps(["not", "an", "object"]),
            )

        db.write_sync(_seed)
        fixed_now = datetime(2026, 1, 5, 0, 0, 0, tzinfo=UTC)
        monkeypatch.setattr(context_module, "utc_now", lambda: fixed_now)
        resp = client.get("/ops")
    assert resp.status_code == 200, resp.text
    html = _norm(resp.text)
    label = "5-minute readings"
    assert (
        f"weather.com access refused: {label} (unreadable hold), "
        "waiting for the next try" in html
    )
    assert "throttled" not in html.lower()
    assert "bad key" not in html.lower()
    close_db()
