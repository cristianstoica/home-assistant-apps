# pyright: strict
"""Shared PASS/FAIL reporting for the ``--check`` topical checks.

`report` prints one ``PASS``/``FAIL`` line per ``(label, bool)`` assertion to
stderr (the HA Log tab stream) plus a ``<TITLE> CHECK PASSED/FAILED`` footer, and
returns the AND — the py-syslog check footer idiom, factored out so each check
module stays declarative.
"""

from __future__ import annotations

import sys


def report(title: str, prefix: str, checks: list[tuple[str, bool]]) -> bool:
    """Print each assertion's PASS/FAIL line + a footer; return all-passed.

    `title` names the check (e.g. ``NAME-ZONE``); `prefix` is the per-line tag
    (e.g. ``name-zone``). Every assertion is printed (no short-circuit) so the
    log shows the full failure surface at once.
    """
    ok = True
    for label, passed in checks:
        print(f"{'PASS' if passed else 'FAIL'}  {prefix}: {label}", file=sys.stderr)
        ok = ok and passed
    print(f"{title} CHECK {'PASSED' if ok else 'FAILED'}", file=sys.stderr)
    return ok
