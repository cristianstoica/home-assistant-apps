"""Tests for the Sites page's subscription-toggle client behaviour (app.js,
the HTML fragment, and the toggle buttons' scoping -- T9-T11).
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
from html.parser import HTMLParser
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db

ROOT = Path(__file__).resolve().parents[1]
APP_JS = (ROOT / "wxverify" / "web" / "static" / "app.js").read_text(encoding="utf-8")
BASE_HTML = (ROOT / "wxverify" / "web" / "templates" / "base.html").read_text(
    encoding="utf-8"
)
FEED_TOGGLES_HTML = (
    ROOT / "wxverify" / "web" / "templates" / "sites" / "_feed_toggles.html"
).read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


def _insert_site(conn: sqlite3.Connection, name: str) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES (?, 40, -105, 900, 'UTC')
            """,
            (name,),
        ).lastrowid
    )


class _AttrParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tags: list[tuple[str, dict[str, str]]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.append((tag, {key: value or "" for key, value in attrs}))


# --------------------------------------------------------------------------
# set_site_subscription, update_subscription and update_feed do not call
# rebuild_mean_for_site at all, so there is no mean-rebuild-skip behavior
# to pin. T9-T11 (app.js/HTML, unrelated to the rebuild) stay.
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# T9 - app.js's subscription-toggle block: the three handlers, and what
# each one does and does not read
# --------------------------------------------------------------------------


_SUB_MARKER = "// Feed subscription toggles:"
_SUB_BLOCK_END_MARKER = '\n  // "Last fetched'


def _sub_block() -> str:
    assert _SUB_MARKER in APP_JS, "app.js lost its subscription toggle block"
    rest = APP_JS.split(_SUB_MARKER, 1)[1]
    assert _SUB_BLOCK_END_MARKER in rest, "could not find the block's own end marker"
    return rest.split(_SUB_BLOCK_END_MARKER, 1)[0]


def _sub_handler(event_name: str) -> str:
    block = _sub_block()
    marker = f'addEventListener("{event_name}", function (event) {{'
    assert marker in block, f"missing the {event_name} subscription handler"
    return block.split(marker, 1)[1].split("\n  });", 1)[0]


def test_t9_app_js_block_has_the_three_handlers_and_outcome_texts() -> None:
    """Catches: the subscription toggle block missing from app.js entirely,
    or missing one of its three listeners or outcome strings, which would
    leave the Sites page silent while a toggle is in flight or after it
    fails."""
    block = _sub_block()
    assert "[data-sub-toggle]" in block
    assert "isConnected" in block
    assert "SUB_BUSY" in block
    assert "SUB_UNKNOWN" in block
    assert "SUB_SESSION" in block
    assert (
        "Applying the change. This can take several minutes; the buttons "
        "stay locked until it finishes."
    ) in block
    assert (
        "No answer yet. The change may still be applying. Wait a few "
        "minutes, then reload the page to see the current state before "
        "clicking again."
    ) in block
    assert "Session expired or request blocked — reload the page to continue" in block
    assert 'addEventListener("htmx:beforeRequest"' in block
    assert 'addEventListener("htmx:beforeSwap"' in block
    assert 'addEventListener("htmx:afterRequest"' in block


def test_t9_before_swap_handler_reads_elt_only_through_requestconfig() -> None:
    """Catches: the beforeSwap handler reading `event.detail.elt` (the swap
    TARGET -- the section, not the button that sent the request) instead of
    `event.detail.requestConfig.elt`, which would misidentify which toggle a
    swap belongs to."""
    handler = _sub_handler("htmx:beforeSwap")
    assert "event.detail.requestConfig" in handler
    assert "event.detail.elt" not in handler
    assert "[data-sub-toggle]" in handler
    assert set(re.findall(r"(\w+)\.elt\b", handler)) == {"requestConfig"}


def test_t9_after_request_handler_leaves_an_unanswered_change_undecided() -> None:
    """Catches: a non-403, non-rejection outcome (no response, a 5xx, a
    swap htmx silently skipped) reported as settled -- changed or
    unchanged -- when the server's actual state cannot be known from the
    client alone."""
    handler = _sub_handler("htmx:afterRequest")
    assert "SUB_UNKNOWN" in handler
    assert "button.disabled = true" in handler
    assert "status === 403" in handler
    assert "400 || status === 404 || status === 422" in handler


# --------------------------------------------------------------------------
# T10 - base.html defers to the toggle's own handler; the fragment
# announces its own outcome
# --------------------------------------------------------------------------


def test_t10_response_error_handler_returns_early_for_subscription_toggles() -> None:
    """Catches: base.html's generic htmx:responseError handler (which shows
    the page-wide "Request rejected"/session-expired notice for every other
    mutation) also firing for a subscription toggle's error response, so its
    notice lands on top of what the toggle's own afterRequest handler
    already wrote into the status line."""
    marker = 'addEventListener("htmx:responseError", function (event) {'
    assert marker in BASE_HTML, "base.html lost its responseError"
    handler = BASE_HTML.split(marker, 1)[1].split("\n      });", 1)[0]
    guard_index = handler.find('elt.matches("[data-sub-toggle]")')
    notice_index = handler.find("mutation-notice")
    assert guard_index != -1
    assert notice_index != -1
    assert guard_index < notice_index


def test_t10_feed_toggles_fragment_has_an_announced_status_line() -> None:
    """Catches: the status line missing `data-sub-status` (app.js's
    `subLine` could never find it to update after a swap), or missing a
    live-region role, which would leave the toggle's outcome unannounced to
    assistive tech."""
    assert "data-sub-status" in FEED_TOGGLES_HTML
    assert 'role="status"' in FEED_TOGGLES_HTML
    assert 'aria-live="polite"' in FEED_TOGGLES_HTML


# --------------------------------------------------------------------------
# T11 - every subscription button's hx-disabled-elt is scoped to its own
# site's section, never narrower or broader
# --------------------------------------------------------------------------


_HX_PUT_SITE_RE = re.compile(r"/api/sites/(\d+)/feeds/\d+")


def test_t11_every_subscription_button_has_a_correctly_scoped_disabled_elt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: `hx-disabled-elt` scoped to `this` (only the clicked button
    locks, letting a second toggle in the same section fire while the first
    is still applying) instead of every button in that site's own
    feed-toggles section; or scoped broadly enough to lock an unrelated
    site's buttons too."""
    close_db()
    config.db_path = str(tmp_path / "wxverify.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        site_a = db.write_sync(lambda conn: _insert_site(conn, "Example Site"))
        site_b = db.write_sync(lambda conn: _insert_site(conn, "Example Site 2"))
        page = client.get("/sites")
    assert page.status_code == 200
    page_text = page.text

    parser = _AttrParser()
    parser.feed(page_text)
    toggle_buttons = [
        attrs
        for tag, attrs in parser.tags
        if tag == "button" and "data-sub-toggle" in attrs
    ]
    buttons_by_site: dict[int, list[dict[str, str]]] = {site_a: [], site_b: []}
    for attrs in toggle_buttons:
        match = _HX_PUT_SITE_RE.search(attrs.get("hx-put", ""))
        assert match, f"button has no recognizable site in hx-put: {attrs}"
        site_id = int(match.group(1))
        assert site_id in buttons_by_site
        buttons_by_site[site_id].append(attrs)

        target = attrs.get("hx-target")
        assert target == f"#feed-toggles-{site_id}"
        assert attrs.get("hx-disabled-elt") == f"{target} button"
        assert attrs.get("hx-ext") == "json-enc"
        assert "X-CSRF-Token" in attrs.get("hx-headers", "")

    for site_id, buttons in buttons_by_site.items():
        assert buttons, f"site {site_id} has no feed toggle buttons"

    for site_id in (site_a, site_b):
        start = page_text.index(
            f'<section class="subsection" id="feed-toggles-{site_id}"'
        )
        end = page_text.index("</section>", start)
        section_text = page_text[start:end]
        assert section_text.count("data-sub-status") == 1
