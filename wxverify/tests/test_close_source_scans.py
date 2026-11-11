"""Source scans (SS1-SS4).

These pin structural facts about the new modules' source text and import
graph directly, the same way test_app_js_chart_a11y.py pins app.js's source
(no JS runner) -- here because the fact under test IS "who calls/imports
whom", which running the code cannot observe any more directly than reading
it. SS1/SS2 additionally import the new names (inside each test body) and
assert they exist, so a world where the files exist but the names do not
still fails for the right reason.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = ROOT / "wxverify"


def _iter_package_py_files() -> list[Path]:
    return sorted(p for p in PACKAGE_ROOT.rglob("*.py") if "__pycache__" not in p.parts)


def _strip_def_lines(text: str, name: str) -> str:
    """``text`` with every line defining ``name`` removed, so a scan for
    calls does not also match the function's own ``def`` line."""
    return "\n".join(line for line in text.splitlines() if f"def {name}(" not in line)


def test_ss1_provisional_site_hours_called_only_from_its_two_homes() -> None:
    """SS1: ``provisional_site_hours`` is called only
    from ``api/routes/timeseries.py`` and ``provisional_line.py`` (M109).

    Also asserts the name exists.
    """
    from wxverify.provisional_line import provisional_site_hours

    assert callable(provisional_site_hours)

    allowed = {
        PACKAGE_ROOT / "api" / "routes" / "timeseries.py",
        PACKAGE_ROOT / "provisional_line.py",
    }
    callers: set[Path] = set()
    for path in _iter_package_py_files():
        text = _strip_def_lines(
            path.read_text(encoding="utf-8"), "provisional_site_hours"
        )
        if "provisional_site_hours(" in text:
            callers.add(path)

    assert callers, (
        "expected at least one real call site (api/routes/timeseries.py"
        " calls it) -- an empty set here means the scan"
        " itself found nothing to check, not that the rule holds."
    )
    extra = callers - allowed
    assert not extra, (
        "M109 -> at this assertion: correct = every caller is one of"
        f" {sorted(p.relative_to(PACKAGE_ROOT) for p in allowed)}, mutant ="
        " a second caller of `provisional_site_hours` elsewhere in the"
        " package, which would show up here as"
        f" {sorted(p.relative_to(PACKAGE_ROOT) for p in extra)}."
    )


def test_ss2_provisional_line_never_imported_by_scoring_or_the_close() -> None:
    """SS2: no module of the close, pairing, scoring,
    rankings, the publication or Station Trust imports ``provisional_line``
    (M110).

    Also asserts the name exists: on today's code this import fails.
    """
    import wxverify.provisional_line as provisional_line_module

    assert provisional_line_module is not None

    allowed_importers = {PACKAGE_ROOT / "api" / "routes" / "timeseries.py"}
    import_pattern = re.compile(r"(?m)^\s*(?:from|import)\s+.*\bprovisional_line\b")
    offenders: set[Path] = set()
    for path in _iter_package_py_files():
        if path == PACKAGE_ROOT / "provisional_line.py":
            continue
        if path in allowed_importers:
            continue
        text = path.read_text(encoding="utf-8")
        if import_pattern.search(text):
            offenders.add(path)

    assert not offenders, (
        "M110 -> at this assertion: correct = no module outside"
        " provisional_line.py's allowed callers imports it, mutant ="
        " `provisional_line` imported by a scoring module, which would"
        f" show up here as {sorted(p.relative_to(PACKAGE_ROOT) for p in offenders)}."
    )

    # Explicit named check on the close/pairing/scoring/rankings/publication
    # modules named here, so the kill is pinned to a concrete list
    # rather than resting only on the broad scan above.
    named_modules = [
        PACKAGE_ROOT / "worker" / "close_chain.py",
        PACKAGE_ROOT / "worker" / "close_p4.py",
        PACKAGE_ROOT / "scoring" / "consensus.py",
        PACKAGE_ROOT / "scoring" / "pairing.py",
        PACKAGE_ROOT / "scoring" / "leaderboard.py",
        PACKAGE_ROOT / "scoring" / "composite.py",
        PACKAGE_ROOT / "scoring" / "winrate.py",
        PACKAGE_ROOT / "scoring" / "publication.py",
        PACKAGE_ROOT / "web" / "context.py",
    ]
    for path in named_modules:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        assert not import_pattern.search(text), (
            f"M110 -> at this assertion: correct = {path.relative_to(PACKAGE_ROOT)}"
            " does not import `provisional_line`, mutant = `provisional_line`"
            f" imported by a scoring module, which would be this file."
        )


def _monitor_import_site(node: ast.AST) -> str | None:
    """A human-readable form of ``node`` if it imports ``wxverify.monitor``
    in any way (module scope or inside a function body), else ``None``."""
    if isinstance(node, ast.Import):
        for alias in node.names:
            if alias.name == "wxverify.monitor":
                return f"import {alias.name}" + (
                    f" as {alias.asname}" if alias.asname else ""
                )
    elif isinstance(node, ast.ImportFrom):
        if node.module == "wxverify.monitor":
            return f"from {node.module} import ..."
        if node.module == "wxverify" and node.level == 0:
            for alias in node.names:
                if alias.name == "monitor":
                    return "from wxverify import monitor" + (
                        f" as {alias.asname}" if alias.asname else ""
                    )
        if node.module is None and node.level >= 1:
            for alias in node.names:
                if alias.name == "monitor":
                    return "." * node.level + " import monitor (relative)"
        if node.module == "monitor" and node.level >= 1:
            return "from " + "." * node.level + "monitor import ... (relative)"
    return None


def test_ss3_close_monitor_is_read_only_and_imports_nothing_from_monitor() -> None:
    """SS3: ``close_monitor.py`` imports nothing from
    ``monitor`` and contains no ``INSERT``, ``UPDATE`` or ``DELETE`` (M111).

    A source regex for ``from wxverify.monitor import`` /
    ``import wxverify.monitor`` misses ``from wxverify import monitor``
    (a different statement: importing the name ``monitor`` from the
    ``wxverify`` package, not the dotted path ``wxverify.monitor``) --
    M111's own planted import is exactly that form, as a function-body
    import, so the original regex-based version of this test could not
    see it. An ``ast`` walk over every node (so a function-body import is
    reached too) catches every import form: ``import wxverify.monitor``,
    ``from wxverify.monitor import ...``, ``from wxverify import monitor``,
    the relative ``from . import monitor`` and the relative
    ``from .monitor import ...``.
    """
    path = PACKAGE_ROOT / "close_monitor.py"
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text, filename=str(path))

    offenders = [
        (node.lineno, site)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        and (site := _monitor_import_site(node)) is not None
    ]
    assert not offenders, (
        "M111 -> at this assertion: correct = close_monitor.py imports"
        " nothing from `monitor`, at module scope or inside a function"
        " body (it copies the stamp helper instead), mutant ="
        " close_monitor.py imports `monitor` (the mutant's own form is a"
        " function-body `from wxverify import monitor as ...`); found"
        f" {offenders!r}."
    )
    # No mutant targets this half of SS1's row on its own (M111's
    # one planted mutant is the `monitor` import, pinned above); this half
    # has no assigned mutant to claim a kill for, so it carries a plain
    # assertion message instead of an "M<n> ->" divergence claim.
    for keyword in ("INSERT", "UPDATE", "DELETE"):
        assert not re.search(rf"\b{keyword}\b", text), (
            f"close_monitor.py must stay read-only (it writes"
            f" nothing); found a bare SQL {keyword} keyword."
        )


def test_ss4_close_monitor_and_provisional_line_read_day_states_through_site_days() -> (
    None
):
    """SS4: ``close_monitor.py`` and
    ``provisional_line.py`` name no day-state table directly, reading day states
    only through ``wxverify.db.site_days``'s functions (M112)."""
    close_monitor = (PACKAGE_ROOT / "close_monitor.py").read_text(encoding="utf-8")
    provisional_line = (PACKAGE_ROOT / "provisional_line.py").read_text(
        encoding="utf-8"
    )

    raw_sql_on_site_days = re.compile(
        r"(?:FROM|INTO|UPDATE)\s+site_days\b", re.IGNORECASE
    )
    site_days_import = re.compile(r"from\s+wxverify\.db\.site_days\s+import")

    for name, text in (
        ("close_monitor.py", close_monitor),
        ("provisional_line.py", provisional_line),
    ):
        assert not raw_sql_on_site_days.search(text), (
            f"M112 -> at this assertion: correct = {name} names no raw SQL"
            " on `site_days` (it reads day states only through"
            " `wxverify.db.site_days`'s functions), mutant ="
            f" {name} reads `site_days` by SQL, which this pattern would"
            " catch."
        )
        assert site_days_import.search(text), (
            f"M112 -> at this assertion: correct = {name} imports its day"
            " states from `wxverify.db.site_days` (the only allowed route),"
            " mutant = close_monitor.py reads `site_days` by SQL"
            f" instead of through that module's functions, so {name} would"
            " carry no such import."
        )
