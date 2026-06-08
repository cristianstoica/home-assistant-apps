# pyright: strict
"""The ``--check`` self-validation oracle and the ``--check --dry-run`` reporter.

`run_check` is the single all-pass entry point ``__main__`` dispatches to: each
topical ``check_*`` builds the production seams against recording fakes, asserts
the produced value equals the declared fixture value, and returns a bool. They
are run through `_guarded` (which also catches an *escaped* exception and records
it as a FAIL rather than aborting the run) and ANDed without short-circuit, so the
report lists *every* failure, not just the first. Exit 0 only when all assertions
hold.

`run_dry_run` loads + validates the options for the configured provider and
prints the **redacted** planned action — never the network, never a secret.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Callable
from urllib.parse import urlparse

from .. import fixtures
from ..redact import sanitize
from .backoff import check_backoff
from .config_checks import (
    check_callback_precedence,
    check_invalid_options,
    check_name_zone,
)
from .confirm import (
    check_api_reconcile,
    check_callback_confirmation,
    check_run_once_never_raises,
    check_startup_self_heal,
)
from .debug_trace import check_debug_trace
from .dryrun import run_dry_run
from .ipparse import check_ip_parse
from .resolve import check_resolver
from .secrets_leak import check_no_secret_leakage
from .shaping import check_url_endpoint_shaping
from .status import check_status_handling

__all__ = ["run_check", "run_dry_run"]

# Mask any embedded ``scheme://host/...`` so an escaped exception can never leak a
# secret-bearing URL path/query, even one the harness doesn't hold a literal for.
_URL_RE = re.compile(r"\b([a-z][a-z0-9+.\-]*)://([^/\s]+)\S*", re.IGNORECASE)

# The fixture secrets the harness can hold as literals (defense in depth on top of
# the URL mask above), so a FAIL line carrying an exception string is scrubbed.
_KNOWN_SECRETS: tuple[str, ...] = (
    fixtures.EXAMPLE_CLIENT_SECRET,
    fixtures.EXAMPLE_URL_SECRET,
    fixtures.EXAMPLE_URL_ENDPOINT,
)


def _redact_failure(message: str) -> str:
    """Scrub an escaped-exception string before it is printed as a FAIL line.

    Replaces any embedded URL with ``<scheme>://<host>/<redacted>`` (the secret
    lives in the path/query) and then literal-scrubs the known fixture secrets.
    An exception (e.g. an ``HttpError``) can echo ``url_endpoint``, which is the
    record-repointing secret, so the FAIL line must go through redaction too.
    """
    masked = _URL_RE.sub(lambda m: f"{m.group(1)}://{m.group(2)}/<redacted>", message)
    return sanitize(masked, _KNOWN_SECRETS)


def _guarded(name: str, fn: Callable[[], bool], ok: bool) -> bool:
    """Run a check, recording an *escaped* exception as a FAIL instead of aborting.

    A check is expected to return its own pass/fail bool (printing its own PASS/
    FAIL lines). If it instead raises (a mis-scripted fake, a struct/index error,
    a regression), the run must not die mid-stream: catch it, print a redacted
    FAIL line, and fold ``False`` into the running result so the run still exits
    non-zero. The redaction is mandatory — an exception can carry a secret URL.
    """
    try:
        return fn() and ok
    except Exception as exc:  # noqa: BLE001 - harness backstop: any escape is a FAIL
        print(
            f"FAIL  {name}: escaped exception: {_redact_failure(str(exc))}",
            file=sys.stderr,
        )
        return False


def run_check() -> int:
    """Dispatch every oracle case (each guarded); exit 0 only when all hold."""
    ok = _guarded("invalid-options", check_invalid_options, True)
    ok = _guarded("name-zone", check_name_zone, ok)
    ok = _guarded("callback-precedence", check_callback_precedence, ok)
    ok = _guarded("url-shaping", check_url_endpoint_shaping, ok)
    ok = _guarded("ip-parse", check_ip_parse, ok)
    ok = _guarded("resolver", check_resolver, ok)
    ok = _guarded("status-handling", check_status_handling, ok)
    ok = _guarded("backoff", check_backoff, ok)
    ok = _guarded("callback-confirm", check_callback_confirmation, ok)
    ok = _guarded("api-reconcile", check_api_reconcile, ok)
    ok = _guarded("run-once-contract", check_run_once_never_raises, ok)
    ok = _guarded("startup-self-heal", check_startup_self_heal, ok)
    ok = _guarded("debug-trace", check_debug_trace, ok)
    ok = _guarded("no-secret-leakage", check_no_secret_leakage, ok)
    ok = _guarded("harness-backstop", check_harness_backstop, ok)
    if ok:
        print("CHECK PASSED", file=sys.stderr)
        return 0
    print("CHECK FAILED", file=sys.stderr)
    return 1


def check_harness_backstop() -> bool:
    """Self-test the escaped-exception backstop and its redaction (GAP 1).

    Runs a deliberately-throwing dummy check through the *same* `_guarded` runner
    and asserts: the runner does not propagate (it returns a bool), it folds to
    ``False`` (the run will exit non-zero), and a thrown exception carrying a
    secret URL is scrubbed by `_redact_failure` before it would be printed.
    """
    from .report import report

    checks: list[tuple[str, bool]] = []

    def _throwing() -> bool:
        raise RuntimeError("deliberate dummy failure")

    # The runner must catch + fold to False, never propagate.
    folded = _guarded("dummy-self-test", _throwing, True)
    checks.append(
        ("escaped exception is folded to FAIL (not propagated)", folded is False)
    )

    # A passing dummy through the runner preserves a True running result.
    preserved = _guarded("dummy-pass", lambda: True, True)
    checks.append(("a passing guarded check keeps the result True", preserved is True))

    # The redaction the FAIL line uses must scrub a secret-bearing URL + literal.
    leaky = (
        f"boom contacting {fixtures.EXAMPLE_URL_ENDPOINT} "
        f"(token {fixtures.EXAMPLE_URL_SECRET})"
    )
    scrubbed = _redact_failure(leaky)
    # Parse the URL the redactor left in the scrubbed message and compare its
    # hostname structurally, not via substring containment — `<host> in <url>`
    # is the classic `py/incomplete-url-substring-sanitization` shape and
    # CodeQL (correctly) won't reason about whether the surrounding context
    # gates a sanitization decision. We do not gate anything on this; we only
    # want to confirm the redactor preserves the host for operator legibility.
    url_match = _URL_RE.search(scrubbed)
    parsed_host = urlparse(url_match.group(0)).hostname if url_match else None
    checks += [
        (
            "redaction masks the secret URL path",
            fixtures.EXAMPLE_URL_ENDPOINT not in scrubbed,
        ),
        (
            "redaction scrubs the bare secret token",
            fixtures.EXAMPLE_URL_SECRET not in scrubbed,
        ),
        (
            "redaction keeps the host (operationally useful)",
            parsed_host == "dynamicdns.example.com",
        ),
    ]
    return report("HARNESS-BACKSTOP", "backstop", checks)
