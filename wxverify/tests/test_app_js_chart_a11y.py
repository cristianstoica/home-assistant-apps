"""Pinning tests for chart canvas accessibility wiring in app.js.

app.js has no JS unit-test runner in this repo (`node --check` is a syntax
gate only), so these assert against the source text directly, the same way
test_version_coherence.py pins config text. They cannot execute the JS or
observe a real DOM; they pin that each uPlot instantiation captures its
return value and feeds it to the labeling helper, and that the helper sets
the expected attributes.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP_JS = (ROOT / "wxverify" / "web" / "static" / "app.js").read_text(encoding="utf-8")


def test_label_chart_canvas_helper_sets_role_and_aria_label() -> None:
    assert "function labelChartCanvas(" in APP_JS
    helper = APP_JS.split("function labelChartCanvas(", 1)[1].split("\n  }", 1)[0]
    assert 'setAttribute("role", "img")' in helper
    assert 'setAttribute("aria-label", label)' in helper


def test_every_uplot_instantiation_captures_return_value() -> None:
    # A discarded `new uPlot(...)` return (bare `new uPlot({` with no
    # assignment) can never be labeled -- this pins that all three chart
    # renderers keep the instance.
    bare_calls = re.findall(r"^\s*new uPlot\(", APP_JS, re.MULTILINE)
    assert bare_calls == [], (
        f"found uPlot(...) call(s) with a discarded return value: {bare_calls!r}"
    )
    assert APP_JS.count("uPlot({") >= 3


def test_each_chart_renderer_calls_label_chart_canvas() -> None:
    calls = re.findall(r'labelChartCanvas\(chart, "([^"]+)"\)', APP_JS)
    assert len(calls) == 3
    # Short orienting labels, not the headline sentence already announced by
    # summaryStatus's .summary-status live region -- would double-announce.
    for label in calls:
        assert "Mean absolute error" not in label
        assert len(label) < 60
