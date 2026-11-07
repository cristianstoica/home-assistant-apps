"""Colour scales for the Forecast page's cells, and the text ink of each fill.

:func:`temp_color`, :func:`wind_color` and :func:`rain_color` port the dark
branches of the design handoff's ``tempColor``, ``windColor`` and
``rainColor`` (``forecast-data.js``). Hue and lightness are rounded to whole
numbers with Python's ``round`` (half to even): at most half a degree or
half a point, which no reader can see. :func:`ink_class` picks each fill's
text ink and darkens a fill that neither ink can read. The templates put the
fill in ``style="background-color:…"``, never the ``background`` shorthand.
Pure: no I/O and no state.
"""

from __future__ import annotations

import colorsys
import re
from typing import Final, Literal, NamedTuple

InkClass = Literal["ink-dark", "ink-light"]

#: ``--surface``, the colour a translucent fill is composited over.
SURFACE_RGB: Final = (0x17, 0x1D, 0x23)
#: The ``ink-dark`` text colour, ``#0f1418``.
INK_DARK_RGB: Final = (0x0F, 0x14, 0x18)
#: The ``ink-light`` text colour, ``--text-on-color`` (``#f4f7f9``).
INK_LIGHT_RGB: Final = (0xF4, 0xF7, 0xF9)
#: The lowest text contrast a fill's ink must reach (WCAG 2.2 AA).
MIN_TEXT_CONTRAST: Final = 4.5

_HSL_RE: Final = re.compile(r"hsl\((\d+) (\d+)% (\d+)%\)")
_RGBA_RE: Final = re.compile(r"rgba\((\d+),(\d+),(\d+),(\d+(?:\.\d+)?)\)")

_RGB = tuple[float, float, float]


class InkedFill(NamedTuple):
    """A cell's fill and the ink class of its text (:func:`ink_class`)."""

    fill: str
    ink: InkClass


def temp_color(temp_c: float) -> str:
    """The fill for a temperature in °C, ``hsl(H 55% 36%)``.

    The hue runs from 240 (blue) at -4 °C and below to 0 (red) at 28 °C
    and above, linear in between.
    """
    hue = 240 - _clamp((temp_c + 4) / 32, 0.0, 1.0) * 240
    return _hsl(round(hue), 55, 36)


def wind_color(wind_kmh: float) -> str | None:
    """The fill for a wind speed in km/h, or ``None`` below 8 km/h.

    With ``k = clamp(wind_kmh / 80, 0, 1)``: hue ``140 - 140 k`` (green to
    red), saturation 65 % and lightness ``28 + 12 k`` %. ``None`` stands for
    the handoff's ``transparent``: no fill.
    """
    if wind_kmh < 8:
        return None
    k = _clamp(wind_kmh / 80, 0.0, 1.0)
    return _hsl(round(140 - k * 140), 65, round(28 + k * 12))


def rain_color(precip_mm: float) -> str | None:
    """The fill for an hourly rain amount in mm, or ``None`` below 0.1 mm.

    ``rgba(2,119,189,A)`` with ``A = clamp(0.15 + precip_mm / 4, 0.15,
    0.95)``, rounded to three decimals. ``None`` stands for the handoff's
    ``transparent``: no fill.
    """
    if precip_mm < 0.1:
        return None
    alpha = _clamp(0.15 + precip_mm / 4, 0.15, 0.95)
    return f"rgba(2,119,189,{round(alpha, 3)})"


def ink_class(css: str) -> InkedFill:
    """The fill to paint for ``css`` and the ink class of its text.

    The fill, composited over ``--surface`` (:data:`SURFACE_RGB`), takes
    ``ink-dark`` or ``ink-light``, whichever has the higher WCAG 2.2
    contrast with it (``ink-light`` on a tie), and is returned unchanged
    when that ink reaches :data:`MIN_TEXT_CONTRAST`. Where neither ink does,
    an ``hsl()`` fill's lightness drops one point at a time until
    ``ink-light`` does, and that darker fill is returned with ``ink-light``.
    An ``rgba()`` fill is never changed: every one :func:`rain_color` emits
    reaches the minimum with ``ink-light``.

    ``css`` must be a colour this module emits: ``hsl(H S% L%)`` in whole
    numbers or ``rgba(R,G,B,A)``. Anything else raises ``ValueError``, so a
    wiring mistake fails loudly instead of picking an ink.
    """
    hsl = _HSL_RE.fullmatch(css)
    if hsl is not None:
        hue, saturation, lightness = (int(group) for group in hsl.groups())
        rgb = _hsl_rgb(hue, saturation, lightness)
        ink = _better_ink(rgb)
        if _contrast(_ink_rgb(ink), rgb) >= MIN_TEXT_CONTRAST:
            return InkedFill(css, ink)
        while lightness > 0 and (
            _contrast(INK_LIGHT_RGB, _hsl_rgb(hue, saturation, lightness))
            < MIN_TEXT_CONTRAST
        ):
            lightness -= 1
        return InkedFill(_hsl(hue, saturation, lightness), "ink-light")
    rgba = _RGBA_RE.fullmatch(css)
    if rgba is None:
        raise ValueError(f"not a colour from web/scales.py: {css!r}")
    alpha = float(rgba.group(4))
    channels = (float(rgba.group(1)), float(rgba.group(2)), float(rgba.group(3)))
    composite = (
        alpha * channels[0] + (1 - alpha) * SURFACE_RGB[0],
        alpha * channels[1] + (1 - alpha) * SURFACE_RGB[1],
        alpha * channels[2] + (1 - alpha) * SURFACE_RGB[2],
    )
    return InkedFill(css, _better_ink(composite))


def _clamp(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def _hsl(hue: int, saturation: int, lightness: int) -> str:
    return f"hsl({hue} {saturation}% {lightness}%)"


def _hsl_rgb(hue: int, saturation: int, lightness: int) -> _RGB:
    """An ``hsl()`` colour's sRGB channels, 0-255 (not rounded)."""
    r, g, b = colorsys.hls_to_rgb(hue / 360, lightness / 100, saturation / 100)
    return (r * 255, g * 255, b * 255)


def _ink_rgb(ink: InkClass) -> _RGB:
    return INK_DARK_RGB if ink == "ink-dark" else INK_LIGHT_RGB


def _better_ink(rgb: _RGB) -> InkClass:
    dark = _contrast(INK_DARK_RGB, rgb)
    light = _contrast(INK_LIGHT_RGB, rgb)
    return "ink-dark" if dark > light else "ink-light"


def _luminance(rgb: _RGB) -> float:
    """WCAG 2.2 relative luminance of sRGB channels in 0-255."""

    def linear(channel: float) -> float:
        c = channel / 255
        return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4

    r, g, b = rgb
    return 0.2126 * linear(r) + 0.7152 * linear(g) + 0.0722 * linear(b)


def _contrast(first: _RGB, second: _RGB) -> float:
    """WCAG 2.2 contrast ratio of two colours, 1 to 21."""
    a, b = _luminance(first), _luminance(second)
    return (max(a, b) + 0.05) / (min(a, b) + 0.05)
