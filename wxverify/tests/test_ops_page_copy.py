"""Item 8 (§1.2/§6.14): the Ops page carries controls, not documentation.

Two scripted deletions removed explanatory prose from the timezone-
correction and publish-hold panels (`ops/_timezone_correction.html`,
`ops/_publish_hold.html`); the publish-hold heading was renamed from
"Nightly Verification Publishing" to "Nightly Verification Run Hold"; and
the one fact the deleted prose carried -- that a run already in progress
continues and may publish while the hold is armed -- was moved into the
arm confirmation dialog in `app.js`.

O42-O45 assert on rendered `/ops` output (and, for the rename sweep,
`README.md`) wherever the branch can be driven, never on template source.

Isolation: HTTP-route oracles drive a real app over `TestClient` against a
file-backed `tmp_path` database with an idle worker stand-in, mirroring
`tests/test_publish_hold_control.py`. O43's five State-cell branches are
driven by monkeypatching `wxverify.web.routes.load_ops` to substitute a
directly-constructed `timezone_correction` row list into the real loader's
own output -- an injected precondition into the real render path (Jinja
templates, real HTTP response), not a template-source read. The building /
failed / cleanup-stalled branches also have full-pipeline coverage in
`tests/test_timezone_correction_control.py`; this file adds the two
branches with none there (published, none) and the ones this item's
concern (deleted prose, surviving ids) actually needs checked together.

Synthetic data only (public repo): `Etc/GMT+7` for the correction target,
no real station or device identifiers.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db
from wxverify.web import context as context_module
from wxverify.web.context import TimezoneCorrectionRow

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


def _override_timezone_correction(
    monkeypatch: pytest.MonkeyPatch, rows: list[TimezoneCorrectionRow]
) -> None:
    """Substitute `rows` for the real loader's `timezone_correction` key,
    leaving every other `/ops` panel on its real (empty-DB) production
    output -- an injected precondition into the real render path, not a
    template-source read."""
    real_load_ops = context_module.load_ops

    def _wrapped(conn: object) -> dict[str, object]:
        ctx = real_load_ops(conn)  # type: ignore[arg-type]
        ctx["timezone_correction"] = rows
        return ctx

    monkeypatch.setattr("wxverify.web.routes.load_ops", _wrapped)


def _row(
    *,
    site_id: int = 1,
    site_name: str = "site-alpha",
    current_timezone: str = "UTC",
    published_generation_id: int | None = None,
    building_generation_id: int | None = None,
    building_timezone: str | None = None,
    failed_generation_id: int | None = None,
    failed_timezone: str | None = None,
    cleanup_stalled_generation_id: int | None = None,
    examined: int | None = None,
    changed: int | None = None,
    unchanged: int | None = None,
    excluded: int | None = None,
    last_published_at: str | None = None,
    applicable: bool = True,
    blocked_reason: str | None = None,
) -> TimezoneCorrectionRow:
    return TimezoneCorrectionRow(
        site_id=site_id,
        site_name=site_name,
        current_timezone=current_timezone,
        published_generation_id=published_generation_id,
        building_generation_id=building_generation_id,
        building_timezone=building_timezone,
        failed_generation_id=failed_generation_id,
        failed_timezone=failed_timezone,
        cleanup_stalled_generation_id=cleanup_stalled_generation_id,
        examined=examined,
        changed=changed,
        unchanged=unchanged,
        excluded=excluded,
        last_published_at=last_published_at,
        applicable=applicable,
        blocked_reason=blocked_reason,
    )


# ---------------------------------------------------------------------------
# O42 -- the deleted prose is gone from the rendered page; both panels
# still render, 200.
# ---------------------------------------------------------------------------

_DELETED_PHRASES = (
    "Retrospective timezone correction",
    "What the flip changes",
    "What happens to verification runs",
    "If it goes wrong",
    "Kill switch",
    "Held blocks NEW runs",
)


def test_o42_deleted_prose_is_absent_and_both_panels_still_render(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o42.db", monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/ops")
    assert resp.status_code == 200, resp.text
    html = resp.text

    for phrase in _DELETED_PHRASES:
        assert phrase not in html, f"deleted prose {phrase!r} still renders"

    assert 'id="timezone-correction-panel"' in html
    assert 'id="publish-hold-panel"' in html
    close_db()


# ---------------------------------------------------------------------------
# O43 -- the timezone-correction panel still does its job: five State-cell
# branches, the input/button, the result placeholder, and the no-sites case.
# ---------------------------------------------------------------------------


def test_o43_building_branch_renders_its_progress_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o43-building.db", monkeypatch)
    _override_timezone_correction(
        monkeypatch,
        [
            _row(
                building_generation_id=7,
                building_timezone="Etc/GMT+7",
                examined=3,
                changed=1,
                unchanged=2,
                excluded=0,
                applicable=False,
                blocked_reason="a correction is already building (generation 7)",
            )
        ],
    )
    with TestClient(app) as client:
        resp = client.get("/ops")
    assert resp.status_code == 200, resp.text
    html = _norm(resp.text)
    assert "building (generation 7" in html
    assert "examined 3" in html
    assert "a correction is already building" in html
    close_db()


def test_o43_published_branch_renders_the_generation_and_timestamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o43-published.db", monkeypatch)
    _override_timezone_correction(
        monkeypatch,
        [
            _row(
                published_generation_id=4,
                last_published_at="2026-07-01T00:00:00Z",
                applicable=True,
            )
        ],
    )
    with TestClient(app) as client:
        resp = client.get("/ops")
    assert resp.status_code == 200, resp.text
    html = _norm(resp.text)
    assert "published generation 4" in html
    assert "2026-07-01T00:00:00Z" in html
    close_db()


def test_o43_none_branch_renders_the_no_published_generation_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o43-none.db", monkeypatch)
    _override_timezone_correction(monkeypatch, [_row(applicable=True)])
    with TestClient(app) as client:
        resp = client.get("/ops")
    assert resp.status_code == 200, resp.text
    html = _norm(resp.text)
    assert "no published generation yet" in html
    close_db()


def test_o43_failed_branch_renders_the_abandonment_notice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o43-failed.db", monkeypatch)
    _override_timezone_correction(
        monkeypatch,
        [
            _row(
                failed_generation_id=9,
                failed_timezone="Etc/GMT+7",
                applicable=True,
            )
        ],
    )
    with TestClient(app) as client:
        resp = client.get("/ops")
    assert resp.status_code == 200, resp.text
    html = _norm(resp.text)
    assert "generation 9" in html
    assert "failed and was abandoned" in html
    close_db()


def test_o43_cleanup_stalled_branch_renders_the_stall_notice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o43-stalled.db", monkeypatch)
    _override_timezone_correction(
        monkeypatch,
        [
            _row(
                published_generation_id=11,
                last_published_at="2026-07-05T00:00:00Z",
                cleanup_stalled_generation_id=11,
                applicable=True,
            )
        ],
    )
    with TestClient(app) as client:
        resp = client.get("/ops")
    assert resp.status_code == 200, resp.text
    html = _norm(resp.text)
    assert "Generation 11 published" in html
    assert "background cleanup stopped" in html
    assert "failed and was abandoned" not in html
    close_db()


def test_o43_applicable_row_renders_input_and_button_not_blocked_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive to the blocked-branch assertion in the building
    test above: an applicable row must render the control, not prose."""
    app = _boot(tmp_path, "o43-applicable.db", monkeypatch)
    _override_timezone_correction(monkeypatch, [_row(site_id=3, applicable=True)])
    with TestClient(app) as client:
        resp = client.get("/ops")
    assert resp.status_code == 200, resp.text
    html = resp.text
    assert 'id="tz-correct-input-3"' in html
    assert "Start correction" in html
    assert 'id="tz-correct-result"' in html
    close_db()


def test_o43_blocked_row_renders_the_reason_not_the_control(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o43-blocked.db", monkeypatch)
    _override_timezone_correction(
        monkeypatch,
        [
            _row(
                site_id=5,
                applicable=False,
                blocked_reason="a verification run is active for this site",
            )
        ],
    )
    with TestClient(app) as client:
        resp = client.get("/ops")
    assert resp.status_code == 200, resp.text
    html = resp.text
    assert 'id="tz-correct-input-5"' not in html
    assert "a verification run is active for this site" in html
    close_db()


def test_o43_no_sites_branch_still_renders_the_panel_heading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _boot(tmp_path, "o43-nosites.db", monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/ops")
    assert resp.status_code == 200, resp.text
    html = resp.text
    assert "No sites configured." in html
    assert 'id="timezone-correction-panel"' in html
    assert 'id="tz-correct-result"' in html
    close_db()


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
