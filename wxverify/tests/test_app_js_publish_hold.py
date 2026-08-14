"""Pinning tests for the Ops publish-hold control's JavaScript (0.11.3).

`app.js` has no JS test runner in this repo, so these assert against its
source text, the same way `tests/test_app_js_chart_a11y.py` does. They
cannot execute the JS or observe a real DOM. What they pin is that a
successful toggle feeds the returned `PublishHoldState` back into every
stateful element of the panel -- `data-held` above all, since it computes
the next click's direction -- that both badges get their class rewritten
alongside their text, and that the banner's two D14 safety variants are
never re-derived in JavaScript.

Two inputs are read from outside `app.js` on purpose, so drift breaks a
test instead of the control: the element list comes from
`ops/_publish_hold.html`, and the payload field names from
`PublishHoldState` itself. The route's half of that contract -- that the
response really is those field names -- is pinned over HTTP in
`tests/test_publish_hold_control.py`.
"""

from __future__ import annotations

import re
from dataclasses import fields
from pathlib import Path

import pytest

from wxverify.verification.publish_hold import PublishHoldState

ROOT = Path(__file__).resolve().parents[1]
APP_JS = (ROOT / "wxverify" / "web" / "static" / "app.js").read_text(encoding="utf-8")
PANEL_HTML = (
    ROOT / "wxverify" / "web" / "templates" / "ops" / "_publish_hold.html"
).read_text(encoding="utf-8")
BANNER_HTML = (
    ROOT / "wxverify" / "web" / "templates" / "_publish_hold_banner.html"
).read_text(encoding="utf-8")

_RENDER_FN = "function renderPublishHold("
_HANDLER_MARKER = "// Publish-hold toggle:"

#: Panel ids the PUT read-back cannot change, so the renderer leaves them:
#: the section wrapper, the handler's own message target, and the bootstrap
#: marker, which is written once by the migration and never again (D2).
_NON_STATEFUL_IDS = {
    "publish-hold-panel",
    "publish-hold-result",
    "publish-hold-bootstrap-marker",
}

_BADGE_IDS = ["publish-hold-state", "publish-hold-chain-active"]

#: Every field a panel control is rendered from.
_REQUIRED_FIELDS = {
    "held",
    "chain_active",
    "last_state",
    "last_source",
    "last_changed_at",
}

#: Word-window width for the banner-duplication scan. Long enough that
#: ordinary shared vocabulary ("verification", "hold") cannot trip it,
#: short enough that copying half a sentence still does.
_WINDOW = 6


def _renderer() -> str:
    """The body of `renderPublishHold`, up to its own two-space closing brace."""
    assert _RENDER_FN in APP_JS, "app.js no longer defines renderPublishHold"
    return APP_JS.split(_RENDER_FN, 1)[1].split("\n  }", 1)[0]


def _handler() -> str:
    """The publish-hold click handler, up to its own two-space `});`."""
    assert _HANDLER_MARKER in APP_JS, "app.js no longer has a publish-hold handler"
    return APP_JS.split(_HANDLER_MARKER, 1)[1].split("\n  });", 1)[0]


def _ok_branch() -> str:
    handler = _handler()
    assert "if (response.ok) {" in handler
    return handler.split("if (response.ok) {", 1)[1].split("} else {", 1)[0]


def _element_var(renderer: str, element_id: str) -> str:
    """The local variable `renderPublishHold` binds `#element_id` to."""
    match = re.search(
        r'var (\w+) = document\.getElementById\("' + re.escape(element_id) + r'"\)',
        renderer,
    )
    assert match is not None, f"renderPublishHold never looks up #{element_id}"
    return match.group(1)


def _words(text: str) -> list[str]:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).split()


def test_success_path_renders_the_state_not_a_reload_instruction() -> None:
    ok_branch = _ok_branch()
    assert "renderPublishHold(payload)" in ok_branch, (
        "a successful toggle leaves every server-rendered control at its "
        "page-load value"
    )
    assert "Reload the page" not in _handler(), (
        "the control must no longer ask the operator to reload by hand"
    )


def test_every_stateful_panel_element_is_rewritten_by_the_renderer() -> None:
    ids = set(re.findall(r'id="(publish-hold-[a-z-]+)"', PANEL_HTML))
    assert ids, "the ops publish-hold panel template declares no ids"
    stateful = ids - _NON_STATEFUL_IDS
    assert stateful == {
        "publish-hold-state",
        "publish-hold-chain-active",
        "publish-hold-last-transition",
        "publish-hold-toggle",
    }, (
        "the panel gained or lost an element: wire it into renderPublishHold, "
        "or record here why the read-back cannot change it"
    )
    renderer = _renderer()
    for element_id in sorted(stateful):
        assert f'document.getElementById("{element_id}")' in renderer, (
            f"#{element_id} would keep its page-load value after a toggle"
        )


@pytest.mark.parametrize("badge_id", _BADGE_IDS)
def test_both_badges_get_their_class_updated_alongside_their_text(
    badge_id: str,
) -> None:
    renderer = _renderer()
    var = _element_var(renderer, badge_id)
    assert f"{var}.textContent =" in renderer
    assert f"{var}.className =" in renderer, (
        f"#{badge_id} would keep its page-load warn/ok/muted class while its "
        "text says otherwise"
    )


def test_toggle_updates_data_held_not_just_its_label() -> None:
    renderer = _renderer()
    var = _element_var(renderer, "publish-hold-toggle")
    assert f"{var}.textContent =" in renderer
    assert f"{var}.dataset.held =" in renderer, (
        "a stale data-held makes the next click re-send the state just set "
        "instead of reversing it"
    )


def test_renderer_reads_only_fields_the_publish_hold_state_declares() -> None:
    renderer = _renderer()
    consumed = set(re.findall(r"payload\.(\w+)", renderer))
    declared = {f.name for f in fields(PublishHoldState)}
    assert consumed <= declared, (
        f"renderPublishHold reads fields the route never returns: "
        f"{sorted(consumed - declared)}"
    )
    assert consumed >= _REQUIRED_FIELDS, (
        f"a panel control is rendered from nothing: "
        f"{sorted(_REQUIRED_FIELDS - consumed)}"
    )


def test_banner_safety_copy_is_not_duplicated_into_javascript() -> None:
    prose = re.sub(r"{#.*?#}", " ", BANNER_HTML, flags=re.DOTALL)
    prose = re.sub(r"{%.*?%}", " ", prose, flags=re.DOTALL)
    prose = re.sub(r"<[^>]+>", " ", prose)
    banner_words = _words(prose)
    assert len(banner_words) > _WINDOW, "the banner template lost its prose"
    app_text = " ".join(_words(APP_JS))
    for start in range(len(banner_words) - _WINDOW + 1):
        window = " ".join(banner_words[start : start + _WINDOW])
        assert window not in app_text, (
            f"app.js repeats the banner's wording ({window!r}); both D14 "
            "variants must stay single-sourced in _publish_hold_banner.html"
        )


def test_renderer_removes_the_banner_and_never_authors_one() -> None:
    renderer = _renderer()
    var = _element_var(renderer, "publish-hold-banner")
    assert f"removeChild({var})" in renderer, (
        "releasing the hold must drop the banner the server rendered"
    )
    assert "createElement" not in renderer
    assert "innerHTML" not in renderer


def test_reload_fallback_is_driven_by_the_two_conditional_elements() -> None:
    renderer = _renderer()
    match = re.search(r"\n    return ([^;]+);", renderer)
    assert match is not None, "renderPublishHold has no final return"
    returned = match.group(1)
    for element_id in ("publish-hold-banner", "publish-hold-last-transition"):
        var = _element_var(renderer, element_id)
        assert var in returned, (
            f"#{element_id} is rendered only by the server when absent, so it "
            "must take part in the reload decision"
        )
    ok_branch = _ok_branch()
    assert "window.location.reload()" in ok_branch, (
        "arming needs a banner this code deliberately cannot author"
    )
