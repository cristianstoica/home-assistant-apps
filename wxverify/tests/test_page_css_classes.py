"""Cross-check that every static class token
anywhere in the Dashboard, Sites, Ops and Overlay template trees -- each
page's own top-level file, every partial it reaches (directly or through
another partial) via ``{% include %}``, and the shared ``base.html`` they
all extend -- is a selector in ``app.css``, or one of the suite's confirmed
JavaScript hooks.

Both the template's class tokens and app.css's class selectors are read by
parsing the actual files, never typed in by hand, so a future rename on
either side shows up here instead of silently drifting out of sync. The
JavaScript-hook allowlist is self-verifying: each entry is checked against
``app.js``'s own source in this file, so a stale allowlist entry (the hook
removed from app.js) fails loudly rather than quietly widening the net.

Each top-level template's tree is walked by following its own
``{% include %}`` directives recursively, so a partial added to one of
these pages later is picked up automatically without a change here. A
pinned set of the partials known to exist today guards that walk itself --
if it ever came back empty or short, that pin fails loudly instead of the
class-token check quietly passing over unscanned markup.

Catches: an unstyled control.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Final

_WEB_ROOT: Final = Path(__file__).resolve().parent.parent / "wxverify" / "web"
_TEMPLATES_ROOT: Final = _WEB_ROOT / "templates"
_APP_CSS: Final = (_WEB_ROOT / "static" / "app.css").read_text(encoding="utf-8")
_APP_JS: Final = (_WEB_ROOT / "static" / "app.js").read_text(encoding="utf-8")

#: The exact templates checked -- each page's own top-level file.
_SHELLS: Final = (
    _TEMPLATES_ROOT / "dashboard" / "show.html",
    _TEMPLATES_ROOT / "sites" / "list.html",
    _TEMPLATES_ROOT / "ops" / "show.html",
    _TEMPLATES_ROOT / "overlay" / "show.html",
)

#: A class token confirmed (below) to be a hook app.js reads/writes rather
#: than a CSS selector. Add an entry ONLY alongside the confirming check in
#: test_summary_status_js_hook_allowlist_entry_is_confirmed_in_app_js.
_JS_HOOK_ALLOWLIST: Final = frozenset({"summary-status"})

#: Every partial (relative to the templates root) known today to be
#: reachable from one of the four shells above, excluding the shells
#: themselves. Pinned so a broken walk -- one that silently stops
#: expanding includes -- fails this test instead of passing over unscanned
#: markup.
_EXPECTED_PARTIALS: Final = frozenset(
    {
        "base.html",
        "dashboard/_composite.html",
        "dashboard/_help.html",
        "dashboard/_leaderboard.html",
        "dashboard/_skillcurve.html",
        "dashboard/_verdict.html",
        "dashboard/_winrate.html",
        "ops/_backfill.html",
        "ops/_budget_gauges.html",
        "ops/_export.html",
        "ops/_feed_status.html",
        "ops/_key_status.html",
        "ops/_observation_health.html",
        "ops/_station_trust.html",
        "ops/_wind_history.html",
        "sites/_card.html",
        "sites/_cards.html",
        "sites/_feed_toggles.html",
        "sites/_form.html",
        "sites/_station_cluster.html",
    }
)


# ---------------------------------------------------------------------------
# CSS selector extraction: strip comments, then collect class tokens from
# the SELECTOR half of every rule only (never a declaration body -- a
# decimal like ".95" in `rgba(2,119,189,.95)` must never be read as a class).
# ---------------------------------------------------------------------------

_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_RULE_RE = re.compile(r"([^{}]+)\{([^{}]*)\}")
_CLASS_TOKEN_RE = re.compile(r"\.([A-Za-z_-][A-Za-z0-9_-]*)")


def _css_class_selectors(css: str) -> set[str]:
    css = _COMMENT_RE.sub("", css)
    selectors_text = " ".join(selectors for selectors, _body in _RULE_RE.findall(css))
    return set(_CLASS_TOKEN_RE.findall(selectors_text))


_CSS_CLASSES: Final = _css_class_selectors(_APP_CSS)


# ---------------------------------------------------------------------------
# Template tree walk: follow every {% include "..." %} directive, recursively,
# starting from each shell, and always add the shared base.html every shell
# extends. A set (not a list) absorbs a partial reached more than once (e.g.
# sites/_card.html from sites/_cards.html) without scanning it twice.
# ---------------------------------------------------------------------------

_INCLUDE_RE = re.compile(r'\{%-?\s*include\s+"([^"]+)"\s*-?%\}')


def _directly_included(path: Path) -> set[str]:
    return set(_INCLUDE_RE.findall(path.read_text(encoding="utf-8")))


def _template_tree(shell: Path) -> set[Path]:
    discovered: set[Path] = {shell, _TEMPLATES_ROOT / "base.html"}
    pending = [shell]
    while pending:
        current = pending.pop()
        for rel in _directly_included(current):
            included = _TEMPLATES_ROOT / rel
            if included not in discovered:
                discovered.add(included)
                pending.append(included)
    return discovered


_TEMPLATES: Final = tuple(
    sorted(
        {template for shell in _SHELLS for template in _template_tree(shell)},
        key=lambda p: str(p.relative_to(_TEMPLATES_ROOT)),
    )
)


# ---------------------------------------------------------------------------
# Template class-attribute extraction: for every class="..." attribute,
# blank out any {{ ... }} Jinja expression first, replacing it with a
# sentinel that is not whitespace rather than a space. That way a fragment
# glued directly onto an expression with no separating space -- e.g.
# `class="panel wide verdict verdict-{{ verdict.state }}"`, where the
# naive split would otherwise slice out a bogus "verdict-" token -- stays
# fused to the sentinel in one token and is dropped, the same as an
# expression that is its own whitespace-separated word.
# ---------------------------------------------------------------------------

_CLASS_ATTR_RE = re.compile(r'class="([^"]*)"')
_JINJA_EXPR_RE = re.compile(r"\{\{.*?\}\}")
_JINJA_FRAGMENT_MARKER = "\x00"


def _template_class_tokens(path: Path) -> list[tuple[str, int]]:
    tokens: list[tuple[str, int]] = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        for match in _CLASS_ATTR_RE.finditer(line):
            cleaned = _JINJA_EXPR_RE.sub(_JINJA_FRAGMENT_MARKER, match.group(1))
            for token in cleaned.split():
                if _JINJA_FRAGMENT_MARKER in token:
                    continue
                tokens.append((token, lineno))
    return tokens


def test_summary_status_js_hook_allowlist_entry_is_confirmed_in_app_js() -> None:
    """Every entry in the allowlist must be a genuine app.js hook, not a
    guess -- confirmed here against app.js's own source rather than asked
    of the author. Catches: a stale allowlist entry naming a hook app.js no
    longer has."""
    for hook in _JS_HOOK_ALLOWLIST:
        assert hook in _APP_JS, f"{hook!r} not found in app.js's source"
    # Specifically: app.js both reads (querySelector) and writes (className)
    # this exact class name, not just a coincidental substring.
    assert '.querySelector(".summary-status")' in _APP_JS
    assert '"summary-status visually-hidden"' in _APP_JS


def test_dashboard_sites_ops_overlay_template_walk_finds_every_known_partial() -> None:
    """The recursive {% include %} walk from the four shells must still
    reach every partial known today (plus base.html) -- pinned so a walk
    that silently stops expanding includes (e.g. a typo in _INCLUDE_RE, or
    an include nested one level deeper than the walk follows) is caught
    here rather than passing the class-token check by skipping markup.
    Catches: a broken walk that scans fewer templates than the pages
    actually render."""
    walked = {str(template.relative_to(_TEMPLATES_ROOT)) for template in _TEMPLATES}
    shells = {str(shell.relative_to(_TEMPLATES_ROOT)) for shell in _SHELLS}
    partials = walked - shells
    assert partials == _EXPECTED_PARTIALS


def test_dashboard_sites_ops_overlay_class_tokens_match_css_or_js_hooks() -> None:
    """Every static class token in the Dashboard, Sites, Ops and Overlay
    template trees -- each shell, every partial it includes (recursively),
    and base.html -- is a selector in app.css or a confirmed JS hook.
    Catches: an unstyled control."""
    unmatched: list[tuple[str, int, str]] = []
    for template in _TEMPLATES:
        rel = str(template.relative_to(_TEMPLATES_ROOT))
        for token, lineno in _template_class_tokens(template):
            if token in _CSS_CLASSES or token in _JS_HOOK_ALLOWLIST:
                continue
            unmatched.append((rel, lineno, token))
    assert unmatched == [], (
        "class token(s) with no app.css selector and no confirmed JS hook: "
        f"{unmatched!r}"
    )
