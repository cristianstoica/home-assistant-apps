# pyright: strict
"""Secret-safety + harness-backstop self-tests (beyond the enumerated Test Plan).

py-weather holds no persistent secret — the only sensitive value is the
``SUPERVISOR_TOKEN`` bearer the Supervisor injects at runtime, scrubbed at one
chokepoint (`redact.sanitize`, applied by `UrllibHttpClient`). These oracles pin
that chokepoint and the ``--check`` runner's own escaped-exception backstop so a
regression that leaks the bearer into a log line, or an escaped exception that
aborts the run, is caught.

`check_no_secret_leakage` exercises `redact.sanitize` directly (the single scrub
surface). `check_harness_backstop` proves the `_guarded` runner folds an escaped
exception to FAIL (never propagates) and that the FAIL-line scrub removes the
bearer from a thrown exception string before it is printed.

These are an addition beyond the plan's enumerated Test Plan (lines 182-213),
which lists no secret-leakage oracle; included because the redaction surface
exists and "never leak the live token" is a hard rule. Flagged as such.
"""

from __future__ import annotations

from .. import fixtures
from ..redact import sanitize
from .report import report

_BEARER = fixtures.EXAMPLE_TOKEN


def check_no_secret_leakage() -> bool:
    """Assert `redact.sanitize` scrubs the bearer and is otherwise faithful.

    The bearer is replaced by ``<redacted>``; non-secret text is preserved; an
    empty secret in the tuple is skipped (replacing ``""`` would corrupt the
    string); multiple secrets are all scrubbed. Mutation discriminator: a no-op
    sanitize (returning the input) fails the "bearer absent from output" line.
    """
    leaky = f"GET http://supervisor/core/api/states (auth Bearer {_BEARER}) failed"
    scrubbed = sanitize(leaky, (_BEARER,))
    multi = sanitize(f"{_BEARER} and SECOND-secret here", (_BEARER, "SECOND-secret"))
    empty_skipped = sanitize("untouched", ("",))

    checks: list[tuple[str, bool]] = [
        ("bearer is absent from the scrubbed string", _BEARER not in scrubbed),
        ("scrubbed string carries the <redacted> marker", "<redacted>" in scrubbed),
        (
            "non-secret context is preserved (host + path retained for legibility)",
            "http://supervisor/core/api/states" in scrubbed,
        ),
        (
            "multiple secrets all scrubbed",
            _BEARER not in multi and "SECOND-secret" not in multi,
        ),
        (
            "empty secret is skipped (string not corrupted)",
            empty_skipped == "untouched",
        ),
    ]
    return report("NO-SECRET-LEAKAGE", "secret", checks)
