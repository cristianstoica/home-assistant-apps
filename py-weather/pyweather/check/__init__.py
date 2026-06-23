# pyright: strict
"""The ``--check`` self-validation oracle (the single all-pass test surface).

`run_check` is the entry point ``__main__`` dispatches to: each topical
``check_*`` builds the production seams against recording fakes, asserts the
produced value equals the declared fixture value, and returns a bool. Each runs
through `_guarded` (which catches an *escaped* exception and records it as a FAIL
rather than aborting the run) and the results are ANDed without short-circuit, so
the report lists *every* failure, not just the first. Exit 0 only when all
assertions hold.

py-weather holds no persistent secret; the only sensitive value is the
``SUPERVISOR_TOKEN`` bearer (injected at runtime). The FAIL-line redaction masks
any embedded URL and scrubs the fixture bearer literal so an escaped exception
can never surface the token — the same defense-in-depth the production
`UrllibHttpClient` applies, mirrored here for the harness.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Callable

from .. import fixtures
from ..redact import sanitize
from .config_checks import (
    check_entity_shape,
    check_invalid_options,
    check_station_key_contract,
    check_valid_defaults,
)
from .discovery_checks import (
    check_discovery_construction_passes_validator,
    check_discovery_merge_and_render,
    check_discovery_transform,
)
from .health_checks import check_freshness, check_health
from .report import report
from .scheduler_checks import (
    check_429_precedence,
    check_backoff_reset_after_recovery,
    check_backoff_sequence,
    check_freshness_reread_recovery,
    check_healthy_interval_bounds,
    check_reward_split,
    check_stop_during_waits,
    check_terminal_path,
    check_transient_path,
)
from .secrets_check import check_no_secret_leakage
from .shaping import check_request_shaping, check_supervisor_request_shaping
from .startup_checks import (
    check_discover_count_stability,
    check_discover_message_discriminators,
    check_discover_retry_and_exit,
    check_persist_allowlist_completeness,
    check_persist_best_effort,
    check_run_startup_branches,
    check_skipped_entity_warnings,
)

__all__ = ["run_check"]

# Mask any embedded ``scheme://host/...`` so an escaped exception can never leak a
# secret-bearing URL (defense in depth; py-weather's URL is the non-secret
# Supervisor proxy, but a future header echo could carry the bearer in a path).
_URL_RE = re.compile(r"\b([a-z][a-z0-9+.\-]*)://([^/\s]+)\S*", re.IGNORECASE)

# The fixture bearer the harness can hold as a literal, so a FAIL line carrying an
# exception string is scrubbed of the token (the sole py-weather secret).
_KNOWN_SECRETS: tuple[str, ...] = (fixtures.EXAMPLE_TOKEN,)


def _redact_failure(message: str) -> str:
    """Scrub an escaped-exception string before it is printed as a FAIL line.

    Masks any embedded URL to ``<scheme>://<host>/<redacted>`` and literal-scrubs
    the fixture bearer. An exception (e.g. an `HttpError`) can echo a request
    header carrying the bearer, so the FAIL line must go through redaction too.
    """
    masked = _URL_RE.sub(lambda m: f"{m.group(1)}://{m.group(2)}/<redacted>", message)
    return sanitize(masked, _KNOWN_SECRETS)


def _guarded(name: str, fn: Callable[[], bool], ok: bool) -> bool:
    """Run a check, recording an *escaped* exception as a FAIL instead of aborting.

    A check returns its own pass/fail bool (printing its own PASS/FAIL lines). If
    it instead raises (a mis-scripted fake, an index error, a regression), the run
    must not die mid-stream: catch it, print a redacted FAIL line, and fold
    ``False`` into the running result so the run still exits non-zero. The
    redaction is mandatory — an exception can carry the bearer.
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
    ok = _guarded("valid-defaults", check_valid_defaults, True)
    ok = _guarded("invalid-options", check_invalid_options, ok)
    ok = _guarded("entity-shape", check_entity_shape, ok)
    ok = _guarded("station-key", check_station_key_contract, ok)
    ok = _guarded("discovery-transform", check_discovery_transform, ok)
    ok = _guarded("discovery-merge", check_discovery_merge_and_render, ok)
    ok = _guarded(
        "discovery-validator", check_discovery_construction_passes_validator, ok
    )
    ok = _guarded("request-shaping", check_request_shaping, ok)
    ok = _guarded("supervisor-shaping", check_supervisor_request_shaping, ok)
    ok = _guarded("persist-allowlist", check_persist_allowlist_completeness, ok)
    ok = _guarded("discover-retry", check_discover_retry_and_exit, ok)
    ok = _guarded("discover-messages", check_discover_message_discriminators, ok)
    ok = _guarded("discover-count", check_discover_count_stability, ok)
    ok = _guarded("persist-best-effort", check_persist_best_effort, ok)
    ok = _guarded("skipped-entity-warnings", check_skipped_entity_warnings, ok)
    ok = _guarded("run-startup", check_run_startup_branches, ok)
    ok = _guarded("health", check_health, ok)
    ok = _guarded("freshness", check_freshness, ok)
    ok = _guarded("freshness-reread", check_freshness_reread_recovery, ok)
    ok = _guarded("reward-split", check_reward_split, ok)
    ok = _guarded("terminal-path", check_terminal_path, ok)
    ok = _guarded("transient-path", check_transient_path, ok)
    ok = _guarded("429-precedence", check_429_precedence, ok)
    ok = _guarded("healthy-interval", check_healthy_interval_bounds, ok)
    ok = _guarded("backoff-sequence", check_backoff_sequence, ok)
    ok = _guarded("backoff-reset", check_backoff_reset_after_recovery, ok)
    ok = _guarded("stop-during-waits", check_stop_during_waits, ok)
    ok = _guarded("no-secret-leakage", check_no_secret_leakage, ok)
    ok = _guarded("harness-backstop", check_harness_backstop, ok)
    if ok:
        print("CHECK PASSED", file=sys.stderr)
        return 0
    print("CHECK FAILED", file=sys.stderr)
    return 1


def check_harness_backstop() -> bool:
    """Self-test the escaped-exception backstop and its bearer redaction.

    Runs a deliberately-throwing dummy check through the *same* `_guarded` runner
    and asserts: the runner folds to ``False`` (never propagates), a passing
    guarded check preserves ``True``, and a thrown exception carrying the bearer
    is scrubbed by `_redact_failure` before it would be printed.
    """
    checks: list[tuple[str, bool]] = []

    def _throwing() -> bool:
        raise RuntimeError("deliberate dummy failure")

    folded = _guarded("dummy-self-test", _throwing, True)
    checks.append(
        ("escaped exception is folded to FAIL (not propagated)", folded is False)
    )

    preserved = _guarded("dummy-pass", lambda: True, True)
    checks.append(("a passing guarded check keeps the result True", preserved is True))

    # A thrown exception carrying the bearer (and a secret-bearing URL) must be
    # scrubbed by the FAIL-line redactor.
    leaky = (
        f"GET http://supervisor/core/api/states failed "
        f"(auth Bearer {fixtures.EXAMPLE_TOKEN})"
    )
    scrubbed = _redact_failure(leaky)
    checks += [
        (
            "redaction scrubs the bearer from a FAIL line",
            fixtures.EXAMPLE_TOKEN not in scrubbed,
        ),
        ("redaction masks the embedded URL", "core/api/states" not in scrubbed),
    ]
    return report("HARNESS-BACKSTOP", "backstop", checks)
