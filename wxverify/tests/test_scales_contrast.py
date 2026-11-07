"""Contrast oracles for the Forecast page's fills (plan step 7, "Contrast").

Every oracle here reimplements WCAG 2.2 relative luminance, contrast ratio,
HSL-to-RGB, hex/rgba parsing and alpha compositing from scratch -- nothing is
imported from ``wxverify.web.scales`` except the colour-producing functions
themselves (``temp_color``, ``wind_color``, ``rain_color``, ``ink_class``),
which ARE the code under test. The oracle's own contrast math is written
independently so a bug in ``scales._contrast``/``_luminance``/``_hsl_rgb``
cannot also be baked into the check that is meant to catch it.

Every background/foreground colour the oracle needs (``--surface``,
``--surface-2``, ``--bg``, ``--muted``, ``--danger-text``, ``--danger-soft``,
``--text-on-color``, the ``.ink-dark``/``.ink-light`` text colours, the
weather-icon mark colours and the timeline rain bar fill) is read out of
``app.css`` itself by parsing the stylesheet, never copied into this file by
hand -- so a changed token value shows up here as a genuine contrast
failure, not as a stale hard-coded expectation.

Catches: a fill neither ink can read, or the handoff bar colour.
"""

from __future__ import annotations

import colorsys
import re
from pathlib import Path
from typing import Final

import pytest

from wxverify.web.scales import ink_class, rain_color, temp_color, wind_color

_APP_CSS: Final = (
    Path(__file__).resolve().parent.parent / "wxverify" / "web" / "static" / "app.css"
).read_text(encoding="utf-8")

_MIN_TEXT_CONTRAST: Final = 4.5
_MIN_NON_TEXT_CONTRAST: Final = 3.0

_RGB = tuple[float, float, float]
_RGBA = tuple[float, float, float, float]

# ---------------------------------------------------------------------------
# A minimal, independent CSS reader: strip comments, split into
# (selector-list, declaration-body) rules, resolve var()/hex/rgba values.
# ---------------------------------------------------------------------------

_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_RULE_RE = re.compile(r"([^{}]+)\{([^{}]*)\}")
_ROOT_VAR_RE = re.compile(r"--([\w-]+):([^;]+)")
_HEX_RE = re.compile(r"#([0-9a-fA-F]{6})\b")
_RGBA_RE = re.compile(r"rgba\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*([\d.]+)\s*\)")
_VARREF_RE = re.compile(r"var\(--([\w-]+)\)")
_HSL_RE = re.compile(r"hsl\((\d+) (\d+)% (\d+)%\)")


def _strip_comments(css: str) -> str:
    return _COMMENT_RE.sub("", css)


def _rules(css: str) -> list[tuple[list[str], str]]:
    out = []
    for selectors, body in _RULE_RE.findall(_strip_comments(css)):
        out.append(([s.strip() for s in selectors.split(",")], body))
    return out


def _root_vars(css: str) -> dict[str, str]:
    for selectors, body in _rules(css):
        if ":root" in selectors:
            return {name: value.strip() for name, value in _ROOT_VAR_RE.findall(body)}
    raise AssertionError(":root rule not found in app.css")


_ROOT_VARS: Final = _root_vars(_APP_CSS)


def _declaration(body: str, prop: str) -> str:
    for part in body.split(";"):
        part = part.strip()
        if part.startswith(prop + ":"):
            return part[len(prop) + 1 :].strip()
    raise AssertionError(f"no {prop!r} declaration in {body!r}")


def _color_for_selector(css: str, selector: str, prop: str) -> str:
    for selectors, body in _rules(css):
        if selector in selectors:
            return _declaration(body, prop)
    raise AssertionError(f"no rule for selector {selector!r} in app.css")


def _parse_rgba(value: str) -> _RGBA:
    """Resolve ``value`` (``var(--x)``, ``#rrggbb`` or ``rgba(r,g,b,a)``) to
    ``(r, g, b, a)`` with r/g/b in 0-255 and a in 0-1, resolving ``var()``
    references against :data:`_ROOT_VARS` recursively."""
    value = value.strip()
    ref = _VARREF_RE.fullmatch(value)
    if ref is not None:
        return _parse_rgba(_ROOT_VARS[ref.group(1)])
    hexm = _HEX_RE.fullmatch(value)
    if hexm is not None:
        h = hexm.group(1)
        return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16), 1.0)
    rgba = _RGBA_RE.fullmatch(value)
    if rgba is not None:
        r, g, b = (float(rgba.group(i)) for i in (1, 2, 3))
        return (r, g, b, float(rgba.group(4)))
    raise AssertionError(f"unrecognized colour token: {value!r}")


def _opaque(value: str) -> _RGB:
    r, g, b, a = _parse_rgba(value)
    assert a == 1.0, f"{value!r} is not opaque"
    return (r, g, b)


def _split_top_level(text: str, sep: str = ",") -> list[str]:
    """Split ``text`` on ``sep`` only outside parentheses (gradient stops'
    colours are themselves comma-separated ``rgba(...)`` calls, so a plain
    ``str.split`` would cut through them)."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == sep and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return parts


def _composite(fg: _RGBA, bg: _RGB) -> _RGB:
    r, g, b, a = fg
    br, bgc, bb = bg
    return (a * r + (1 - a) * br, a * g + (1 - a) * bgc, a * b + (1 - a) * bb)


def _hsl_to_rgb(css: str) -> _RGB:
    m = _HSL_RE.fullmatch(css)
    assert m is not None, f"not an hsl() colour: {css!r}"
    hue, saturation, lightness = (int(g) for g in m.groups())
    r, g, b = colorsys.hls_to_rgb(hue / 360, lightness / 100, saturation / 100)
    return (r * 255, g * 255, b * 255)


def _luminance(rgb: _RGB) -> float:
    """WCAG 2.2 relative luminance of sRGB channels in 0-255 (own
    reimplementation, not imported from scales.py)."""

    def linear(channel: float) -> float:
        c = channel / 255
        return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4

    r, g, b = rgb
    return 0.2126 * linear(r) + 0.7152 * linear(g) + 0.0722 * linear(b)


def _contrast(first: _RGB, second: _RGB) -> float:
    """WCAG 2.2 contrast ratio, 1 to 21 (own reimplementation)."""
    a, b = _luminance(first), _luminance(second)
    return (max(a, b) + 0.05) / (min(a, b) + 0.05)


# ---------------------------------------------------------------------------
# App.css-derived colours used throughout.
# ---------------------------------------------------------------------------

_SURFACE: Final = _opaque(_ROOT_VARS["surface"])
_SURFACE_2: Final = _opaque(_ROOT_VARS["surface-2"])
_BG: Final = _opaque(_ROOT_VARS["bg"])
_MUTED: Final = _opaque(_ROOT_VARS["muted"])
_DANGER_TEXT: Final = _opaque(_ROOT_VARS["danger-text"])
_DANGER_SOFT: Final = _parse_rgba(_ROOT_VARS["danger-soft"])

_INK_DARK: Final = _opaque(_color_for_selector(_APP_CSS, ".ink-dark", "color"))
_INK_LIGHT: Final = _opaque(_color_for_selector(_APP_CSS, ".ink-light", "color"))


def _ink_rgb(ink: str) -> _RGB:
    return _INK_DARK if ink == "ink-dark" else _INK_LIGHT


# ---------------------------------------------------------------------------
# Exhaustive sweep over every fill web/scales.py can emit.
# ---------------------------------------------------------------------------


def _fill_to_rgb(css: str) -> _RGB:
    if css.startswith("hsl("):
        return _hsl_to_rgb(css)
    rgba = _parse_rgba(css)
    return _composite(rgba, _SURFACE)


def test_every_temp_color_ink_reaches_minimum_contrast() -> None:
    """Temperature fills, -40..50 C in 0.1 steps. Catches: a fill neither
    ink can read."""
    checked = 0
    for tenth in range(-400, 501):
        temp_c = tenth / 10
        css = temp_color(temp_c)
        fill, ink = ink_class(css)
        rgb = _fill_to_rgb(fill)
        contrast = _contrast(_ink_rgb(ink), rgb)
        assert contrast >= _MIN_TEXT_CONTRAST - 1e-9, (
            f"temp_color({temp_c}) -> {fill!r}/{ink}: contrast {contrast:.4f}"
        )
        checked += 1
    assert checked == 901


def test_every_wind_color_ink_reaches_minimum_contrast() -> None:
    """Wind fills, 0..200 km/h in 0.1 steps (below 8 km/h has no fill).
    Catches: a fill neither ink can read."""
    checked = 0
    for tenth in range(0, 2001):
        wind_kmh = tenth / 10
        css = wind_color(wind_kmh)
        if css is None:
            continue
        fill, ink = ink_class(css)
        rgb = _fill_to_rgb(fill)
        contrast = _contrast(_ink_rgb(ink), rgb)
        assert contrast >= _MIN_TEXT_CONTRAST - 1e-9, (
            f"wind_color({wind_kmh}) -> {fill!r}/{ink}: contrast {contrast:.4f}"
        )
        checked += 1
    assert checked > 0


def test_every_rain_color_ink_reaches_minimum_contrast() -> None:
    """Rain fills, 0..60 mm in 0.01 steps (below 0.1 mm has no fill).
    Catches: a fill neither ink can read."""
    checked = 0
    for hundredth in range(0, 6001):
        precip_mm = hundredth / 100
        css = rain_color(precip_mm)
        if css is None:
            continue
        fill, ink = ink_class(css)
        rgb = _fill_to_rgb(fill)
        contrast = _contrast(_ink_rgb(ink), rgb)
        assert contrast >= _MIN_TEXT_CONTRAST - 1e-9, (
            f"rain_color({precip_mm}) -> {fill!r}/{ink}: contrast {contrast:.4f}"
        )
        checked += 1
    assert checked > 0


# ---------------------------------------------------------------------------
# --muted on --surface and --surface-2; --danger-text on --danger-soft over
# --bg.
# ---------------------------------------------------------------------------


def test_muted_text_reaches_minimum_contrast_on_surface_and_surface_2() -> None:
    """--muted text over --surface and --surface-2. Catches: a muted text
    token too close to one of its two backing surfaces."""
    for name, surface in (("--surface", _SURFACE), ("--surface-2", _SURFACE_2)):
        contrast = _contrast(_MUTED, surface)
        assert contrast >= _MIN_TEXT_CONTRAST - 1e-9, (
            f"--muted on {name}: contrast {contrast:.4f}"
        )


def test_danger_text_on_danger_soft_over_bg_reaches_minimum_contrast() -> None:
    """--danger-text over --danger-soft composited onto --bg (the .notice
    panel's own stack). Catches: a danger-soft/danger-text pair that reads
    fine on paper but not once alpha-blended onto the page background."""
    composite = _composite(_DANGER_SOFT, _BG)
    contrast = _contrast(_DANGER_TEXT, composite)
    assert contrast >= _MIN_TEXT_CONTRAST - 1e-9, (
        f"--danger-text on --danger-soft/--bg: contrast {contrast:.4f}"
    )


# ---------------------------------------------------------------------------
# Weather-icon marks, >= 3:1 against --surface (WCAG 2.2 1.4.11 non-text
# contrast -- these are graphical marks, not body text).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("selector", "prop"),
    [
        (".ic-sun::before", "border"),
        (".ic-partly::before", "border"),
        (".ic-partly::after", "background"),
        (".ic-cloud::before", "background"),
        (".ic-rain::before", "background"),
        (".ic-heavy::before", "background"),
    ],
)
def test_icon_mark_reaches_minimum_contrast_against_surface(
    selector: str, prop: str
) -> None:
    """Each weather-icon mark's border/background colour against --surface.
    Catches: an icon mark too close to its own backing surface to read as a
    distinct shape."""
    value = _color_for_selector(_APP_CSS, selector, prop)
    # `border` declarations are "<width> <style> <color>"; `background`
    # declarations are a bare colour token.
    token = value.split()[-1]
    rgb = _opaque(token)
    contrast = _contrast(rgb, _SURFACE)
    assert contrast >= _MIN_NON_TEXT_CONTRAST - 1e-9, (
        f"{selector} {prop} {token!r}: contrast {contrast:.4f} against --surface"
    )


# ---------------------------------------------------------------------------
# The timeline rain bar (.tl .cell.rain): its own fill against --surface
# (non-text, the handoff bar), and its text (.tl .cell's --text-on-color)
# against that fill.
# ---------------------------------------------------------------------------


def test_timeline_rain_bar_text_and_bar_reach_their_minimum_contrasts() -> None:
    """The timeline rain bar's fill vs. --surface (>=3:1, a visible bar) and
    its text vs. the fill (>=4.5:1). Catches: the handoff bar colour."""
    gradient = _color_for_selector(_APP_CSS, ".tl .cell.rain", "background")
    # linear-gradient(to top, <bar-color> var(--h), var(--surface) var(--h))
    stops_text = gradient[gradient.index("(") + 1 : gradient.rindex(")")]
    parts = _split_top_level(stops_text)
    assert len(parts) == 3, f"unexpected gradient stop count: {parts!r}"
    first_stop = parts[1].strip()
    bar_color_token = first_stop.rsplit(" ", 1)[0].strip()
    bar_rgba = _parse_rgba(bar_color_token)
    bar_over_surface = _composite(bar_rgba, _SURFACE)

    bar_contrast = _contrast(bar_over_surface, _SURFACE)
    assert bar_contrast >= _MIN_NON_TEXT_CONTRAST - 1e-9, (
        f"timeline rain bar {bar_color_token!r} vs --surface:"
        f" contrast {bar_contrast:.4f}"
    )

    text_token = _color_for_selector(_APP_CSS, ".tl .cell", "color")
    text_rgb = _opaque(text_token)
    text_contrast = _contrast(text_rgb, bar_over_surface)
    assert text_contrast >= _MIN_TEXT_CONTRAST - 1e-9, (
        f"timeline rain bar text {text_token!r} vs bar fill:"
        f" contrast {text_contrast:.4f}"
    )
