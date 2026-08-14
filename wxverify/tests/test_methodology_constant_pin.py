"""§14/W11: the automated pin on `verification/methodology.py`.

Every public constant declared there is either referenced by a non-test
module or named in ``DECLARATIVE_ONLY``. This is the test that stops the
drift W11 repaired from recurring: a constant published as methodology but
wired to nothing is either consumed or declared, never silently neither.

The scan is AST-based rather than textual, so a constant's name appearing
inside a docstring, a comment or an unrelated string literal does not count
as a consumer.
"""

from __future__ import annotations

import ast
from pathlib import Path

from wxverify.verification.methodology import DECLARATIVE_ONLY

_PACKAGE = Path(__file__).resolve().parents[1] / "wxverify"
_MODULE = _PACKAGE / "verification" / "methodology.py"

#: The pin registry is itself public and, by construction, referenced only
#: by this test. Excluding it keeps the pin from demanding that it declare
#: itself declarative-only.
_EXEMPT = frozenset({"DECLARATIVE_ONLY"})


def _declared_constants() -> set[str]:
    tree = ast.parse(_MODULE.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in tree.body:
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        for target in targets:
            if isinstance(target, ast.Name) and not target.id.startswith("_"):
                names.add(target.id)
    return names - _EXEMPT


def _referenced_names() -> set[str]:
    """Every identifier any other non-test module in the package mentions.

    Both spellings count: a bare ``ROSTER_AVAILABILITY_FLOOR`` from a
    ``from ... import`` and the ``methodology.ROSTER_AVAILABILITY_FLOOR``
    attribute access.
    """
    seen: set[str] = set()
    for path in sorted(_PACKAGE.rglob("*.py")):
        if path == _MODULE:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                seen.add(node.id)
            elif isinstance(node, ast.Attribute):
                seen.add(node.attr)
            elif isinstance(node, ast.alias):
                seen.add(node.name.rsplit(".", 1)[-1])
    return seen


def test_every_public_constant_is_consumed_or_declared_declarative_only() -> None:
    declared = _declared_constants()
    referenced = _referenced_names()

    unaccounted = sorted(declared - referenced - set(DECLARATIVE_ONLY))

    assert not unaccounted, (
        "methodology constants that no non-test module consumes and that "
        f"DECLARATIVE_ONLY does not declare: {unaccounted}. Wire them, "
        "delete them, or add them to DECLARATIVE_ONLY with a reason."
    )


def test_declarative_only_names_real_and_genuinely_unwired_constants() -> None:
    """The registry cannot be used to silence a live constant, and cannot
    outlive the constant it names."""
    declared = _declared_constants()
    referenced = _referenced_names()

    assert set(DECLARATIVE_ONLY) <= declared, (
        "DECLARATIVE_ONLY names something methodology.py does not declare: "
        f"{sorted(set(DECLARATIVE_ONLY) - declared)}"
    )
    still_consumed = sorted(set(DECLARATIVE_ONLY) & referenced)
    assert not still_consumed, (
        f"DECLARATIVE_ONLY declares constants that ARE consumed: {still_consumed}"
    )


def test_the_scan_finds_a_real_consumer_and_a_real_orphan() -> None:
    """Negative control for the scan itself: a green pin above must mean
    'nothing is unaccounted for', not 'the scan sees nothing'."""
    declared = _declared_constants()
    referenced = _referenced_names()

    assert "ROSTER_AVAILABILITY_FLOOR" in declared
    assert "ROSTER_AVAILABILITY_FLOOR" in referenced
    # A name that has never existed in the package is genuinely absent, so
    # the reference set is a filter and not a catch-all.
    assert "ROSTER_AVAILABILITY_FLOOR_SENSITIVITY" not in referenced
    assert "ROSTER_AVAILABILITY_FLOOR_SENSITIVITY" not in declared
