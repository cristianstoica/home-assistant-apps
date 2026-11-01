"""Item 8 (§1.2/§6.14): the Ops page carries controls, not documentation.

A scripted deletion removed explanatory prose from the publish-hold panel
(`ops/_publish_hold.html`); the publish-hold heading was renamed from
"Nightly Verification Publishing" to "Nightly Verification Run Hold"; and
the one fact the deleted prose carried -- that a run already in progress
continues and may publish while the hold is armed -- was moved into the
arm confirmation dialog in `app.js`. The Ops timezone-correction panel
this file's O42/O43 tests once covered was removed in 0.16.14 Part 6; its
own coverage went with it.

O44-O45 assert on rendered `/ops` output (and, for the rename sweep,
`README.md`) wherever the branch can be driven, never on template source.

Isolation: HTTP-route oracles drive a real app over `TestClient` against a
file-backed `tmp_path` database with an idle worker stand-in, mirroring
`tests/test_publish_hold_control.py`.

Synthetic data only (public repo): no real station or device identifiers.
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import replace
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
# Harness (mirrors tests/test_publish_hold_control.py).
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
# O44 -- the publish-hold panel keeps every element; the rename is swept.
# ---------------------------------------------------------------------------

_PUBLISH_HOLD_ID_RE = re.compile(r'id="(publish-hold-[a-z-]+)"')

_EXPECTED_PUBLISH_HOLD_IDS = frozenset(
    {
        "publish-hold-panel",
        "publish-hold-state",
        "publish-hold-chain-active",
        "publish-hold-last-transition",
        "publish-hold-bootstrap-marker",
        "publish-hold-toggle",
        "publish-hold-result",
        "publish-hold-banner",
    }
)


def test_o44_heading_renamed_ids_intact_and_readme_swept(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The fixture forces `publish_hold.held`, `.last_state` and
    `.bootstrap` all truthy so every conditional id renders, matching the
    eight-id set named in the plan."""
    real_load_ops = context_module.load_ops

    def _wrapped(conn: object) -> dict[str, object]:
        ctx = real_load_ops(conn)  # type: ignore[arg-type]
        ctx["publish_hold"] = replace(
            ctx["publish_hold"],
            held=True,
            chain_active=False,
            last_state="held",
            last_source="ops",
            last_changed_at="2026-07-01T00:00:00Z",
            bootstrap="install",
        )
        return ctx

    monkeypatch.setattr("wxverify.web.routes.load_ops", _wrapped)
    app = _boot(tmp_path, "o44.db", monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/ops")
    assert resp.status_code == 200, resp.text
    html = resp.text

    assert "Nightly Verification Run Hold" in html
    assert "Nightly Verification Publishing" not in html

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "Nightly Verification Publishing" not in readme

    for badge_id in (
        "publish-hold-state",
        "publish-hold-chain-active",
        "publish-hold-last-transition",
        "publish-hold-bootstrap-marker",
        "publish-hold-toggle",
        "publish-hold-result",
        "publish-hold-banner",
    ):
        assert f'id="{badge_id}"' in html, f"missing surviving element {badge_id!r}"

    found_ids = frozenset(_PUBLISH_HOLD_ID_RE.findall(html))
    assert found_ids == _EXPECTED_PUBLISH_HOLD_IDS
    close_db()


# ---------------------------------------------------------------------------
# O45 -- the dropped fact lives in the arm dialog; the banner-duplication
# guard is untouched by this item's edits.
# ---------------------------------------------------------------------------

_ARM_STRING = (
    "Hold nightly verification runs? New runs will not start until "
    "released. A run already in progress continues and may publish."
)
_RELEASE_STRING = "Release the nightly verification run hold?"


def test_o45_arm_dialog_carries_the_in_progress_fact() -> None:
    app_js = (ROOT / "wxverify" / "web" / "static" / "app.js").read_text(
        encoding="utf-8"
    )
    assert _ARM_STRING in app_js
    assert _RELEASE_STRING in app_js
    assert "A run already in progress continues and may publish." in app_js


def test_o45_banner_duplication_guard_still_passes_unedited() -> None:
    """Imports and re-runs `test_app_js_publish_hold.py`'s own guard
    function directly, proving this item did not need to touch it -- if the
    arm string had drifted into the banner's 6-word window, the string
    would be wrong, not this test."""
    import tests.test_app_js_publish_hold as guard_module

    guard_module.test_banner_safety_copy_is_not_duplicated_into_javascript()


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
