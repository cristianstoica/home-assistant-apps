# pyright: strict
"""Config-surface checks: invalid-options rejection and the name↔zone contract."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from .. import config, fixtures
from ..config import ConfigError, derive_record_label
from .report import report


def check_invalid_options() -> bool:
    """Assert every `INVALID_OPTIONS` payload is rejected, naming the field.

    Two layers, mirroring py-syslog:

    1. **Field validation** — each payload through `config.validate` raises a
       `ConfigError` whose message contains the expected field token
       (per-provider required fields, HTTPS-only contract on both
       ``url_endpoint`` and ``ip_source_urls[i]``, name↔zone apex/wrong-zone,
       range/enum checks).
    2. **File loading** — `config.load` rejects malformed JSON, a non-object
       top-level value, and a missing path, each naming the cause.
    """
    checks: list[tuple[str, bool]] = []
    for fixture in fixtures.INVALID_OPTIONS:
        try:
            config.validate(fixture.options)
        except ConfigError as exc:
            passed = fixture.field in str(exc)
            checks.append(
                (f"[{fixture.name}] rejected naming {fixture.field!r}", passed)
            )
            if not passed:
                print(
                    f"  (got {str(exc)!r}, expected to name {fixture.field!r})",
                    file=sys.stderr,
                )
        else:
            checks.append((f"[{fixture.name}] raised ConfigError", False))
    ok = report("INVALID-OPTIONS", "invalid-options", checks)
    return _check_load_negatives() and ok


def _check_load_negatives() -> bool:
    """Assert `config.load` rejects bad files with a cause-naming `ConfigError`."""
    checks: list[tuple[str, bool]] = []

    def _assert_load_error(name: str, content: str, cause: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            opts = Path(tmp) / "options.json"
            opts.write_text(content, encoding="utf-8")
            try:
                config.load(str(opts))
            except ConfigError as exc:
                checks.append((f"load [{name}] names {cause!r}", cause in str(exc)))
            else:
                checks.append((f"load [{name}] raised ConfigError", False))

    _assert_load_error("malformed JSON", "{ not json", "invalid JSON")
    _assert_load_error(
        "top-level array", '["a", "b"]', "top-level value must be an object"
    )
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "does-not-exist.json"
        try:
            config.load(str(missing))
        except ConfigError as exc:
            checks.append(
                ("load [missing path] names 'cannot read'", "cannot read" in str(exc))
            )
        else:
            checks.append(("load [missing path] raised ConfigError", False))
    return report("LOAD-NEGATIVES", "load-negative", checks)


def check_name_zone() -> bool:
    """Assert the name↔zone derivation: accepts derive the label, rejects name it.

    Drives `derive_record_label` directly (the contract chokepoint) against the
    `NAME_ZONE_CASES` corpus: a valid sub-record derives the relative label
    (case- and trailing-dot-insensitive); the zone apex, a wrong-zone name, and
    an empty name are each rejected with the expected substring.
    """
    checks: list[tuple[str, bool]] = []
    for case in fixtures.NAME_ZONE_CASES:
        try:
            label = derive_record_label(case.name, case.zone)
        except ConfigError as exc:
            if case.expected_reject is not None:
                checks.append(
                    (
                        f"{case.name!r}/{case.zone!r} rejected ({case.expected_reject!r})",
                        case.expected_reject in str(exc),
                    )
                )
            else:
                checks.append(
                    (
                        f"{case.name!r}/{case.zone!r} should derive {case.expected_label!r}",
                        False,
                    )
                )
                print(f"  (unexpected reject: {exc})", file=sys.stderr)
        else:
            if case.expected_label is not None:
                checks.append(
                    (
                        f"{case.name!r}/{case.zone!r} -> label {case.expected_label!r}",
                        label == case.expected_label,
                    )
                )
            else:
                checks.append(
                    (
                        f"{case.name!r}/{case.zone!r} should reject ({case.expected_reject!r})",
                        False,
                    )
                )
                print(f"  (unexpectedly derived label {label!r})", file=sys.stderr)
    return report("NAME-ZONE", "name-zone", checks)
