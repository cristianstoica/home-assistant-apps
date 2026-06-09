# pyright: strict
"""Debug-trace check: the per-cycle trace is real, level-gated, and secret-safe.

The ``log_level`` field promises that ``debug`` "additionally traces each cycle's
IP detection, update decision and confirmation outcome". This check gives that
promise teeth on the production `Updater.run_once` path:

* a normal cycle emits all three trace lines **at DEBUG** (both archetypes);
* with the logger threshold raised to **INFO**, none of those debug lines is
  created (the trace is debug-only — it must stay silent at the production
  default);
* no captured debug record leaks a secret — the callback `url_endpoint` (a
  capability URL whose path *is* the credential) and the Azure `client_secret`
  appear in **none** of the debug output, even when the updater is configured
  with the real fixture secret.

The trace lines are identified by the stable phrases the emitters use
(``ip detection`` / ``update decision`` / ``confirmation``), not by exact text,
so wording can evolve without making this check brittle.
"""

from __future__ import annotations

import logging
from ipaddress import IPv4Address

from .. import fixtures
from ..models import (
    ApplyAction,
    ApplyResult,
    AzureToken,
    Config,
    Provider,
    ResolveOutcome,
    ResolveStatus,
)
from ..updater import Updater
from .fakes import (
    FakeClock,
    FakeIpSource,
    FakeProvider,
    FakeResolver,
    FakeSleeper,
    FakeState,
    capture_at_level,
)
from .report import report

# The trace-line discriminators, one per lifecycle stage the en.yaml promise names.
_TRACE_PHRASES = ("ip detection", "update decision", "confirmation")

# The secrets that must never reach a debug record (same set as secrets_leak.py).
_SECRETS = (
    fixtures.EXAMPLE_URL_SECRET,
    fixtures.EXAMPLE_CLIENT_SECRET,
    fixtures.EXAMPLE_URL_ENDPOINT,
)

_IP_NEW = IPv4Address("203.0.113.50")


def _leaks(text: str) -> bool:
    """True if any tracked secret substring appears in `text`."""
    return any(secret and secret in text for secret in _SECRETS)


def _url_config() -> Config:
    """A ``url`` Config carrying the *real* fixture secret in `url_endpoint`.

    Using the live secret endpoint (not a placeholder) means a leak into any debug
    record would actually be detectable by `_leaks`.
    """
    return Config(
        provider=Provider.URL,
        name="home.example.com",
        test_ns="",
        azure=None,
        record_label="",
        url_endpoint=fixtures.EXAMPLE_URL_ENDPOINT,
        url_send_myip=False,
        url_insecure_skip_verify=False,
        ttl=60,
        interval_seconds=120,
        drift_reconcile_seconds=0,
        ip_source_urls=("https://api.ipify.org",),
        log_level="debug",
        state_path="/data/last_known_ip",
    )


def _azure_config() -> Config:
    """An ``azure`` Config carrying the *real* fixture client_secret."""
    return Config(
        provider=Provider.AZURE,
        name="home.example.com",
        test_ns="",
        azure=AzureToken(
            tenant_id="t",
            subscription_id="sub",
            resource_group="rg",
            zone="example.com",
            client_id="cid",
            client_secret=fixtures.EXAMPLE_CLIENT_SECRET,
        ),
        record_label="home",
        url_endpoint="",
        url_send_myip=False,
        url_insecure_skip_verify=False,
        ttl=60,
        interval_seconds=120,
        drift_reconcile_seconds=0,
        ip_source_urls=("https://api.ipify.org",),
        log_level="debug",
        state_path="/data/last_known_ip",
    )


def _url_updater(config: Config) -> Updater:
    """A ``url`` updater whose first cycle fires + confirms to a resolved value."""
    return Updater(
        config,
        ip_source=FakeIpSource(_IP_NEW),  # type: ignore[arg-type]
        provider=FakeProvider(  # type: ignore[arg-type]
            apply_result=ApplyResult(ApplyAction.FIRED_SERVER_DETECTED, "fired", None)
        ),
        resolver=FakeResolver(  # type: ignore[arg-type]
            ResolveOutcome(ResolveStatus.RESOLVED, _IP_NEW)  # post-fire confirm
        ),
        state=FakeState(),  # empty -> first cycle authoritative -> fire+confirm
        clock=FakeClock(),
        sleeper=FakeSleeper(),
    )


def _azure_updater(config: Config) -> Updater:
    """An ``azure`` updater whose first cycle reads, applies, and persists."""
    return Updater(
        config,
        ip_source=FakeIpSource(_IP_NEW),  # type: ignore[arg-type]
        provider=FakeProvider(  # type: ignore[arg-type]
            read_result=None,  # missing record -> authoritative apply
            apply_result=ApplyResult(ApplyAction.WROTE_KNOWN_IP, "wrote", None),
        ),
        resolver=FakeResolver(),  # type: ignore[arg-type]  # unused on the API path
        state=FakeState(),
        clock=FakeClock(),
        sleeper=FakeSleeper(),
    )


def _traced_phrases(records: list[tuple[int, str]], at_level: int) -> set[str]:
    """The trace phrases present in records emitted *exactly* at `at_level`."""
    found: set[str] = set()
    for levelno, message in records:
        if levelno != at_level:
            continue
        for phrase in _TRACE_PHRASES:
            if phrase in message:
                found.add(phrase)
    return found


def check_debug_trace() -> bool:
    """Assert the per-cycle debug trace is real, level-gated, and secret-safe."""
    checks: list[tuple[str, bool]] = []

    for label, build_updater, config in (
        ("url", _url_updater, _url_config()),
        ("azure", _azure_updater, _azure_config()),
    ):
        # --- at DEBUG: all three lifecycle trace lines are emitted ---
        debug_handler = capture_at_level(
            logging.DEBUG, lambda _h, _b=build_updater, _c=config: _b(_c).run_once()
        )
        debug_phrases = _traced_phrases(debug_handler.records, logging.DEBUG)
        for phrase in _TRACE_PHRASES:
            checks.append(
                (
                    f"{label}: '{phrase}' traced at DEBUG",
                    phrase in debug_phrases,
                )
            )

        # --- at INFO: none of the debug trace lines is created ---
        info_handler = capture_at_level(
            logging.INFO, lambda _h, _b=build_updater, _c=config: _b(_c).run_once()
        )
        debug_records_at_info = [
            m for lvl, m in info_handler.records if lvl == logging.DEBUG
        ]
        checks.append(
            (
                f"{label}: no debug record emitted at INFO threshold",
                debug_records_at_info == [],
            )
        )
        # The INFO cycle still produces its normal per-cycle outcome line (the
        # trace being silent must not have silenced the lifecycle itself).
        checks.append(
            (
                f"{label}: INFO lifecycle line still emitted (trace silenced, not cycle)",
                any(lvl == logging.INFO for lvl, _ in info_handler.records),
            )
        )

        # --- secret-safety: no debug record leaks a secret ---
        checks.append(
            (
                f"{label}: no debug record leaks a secret",
                not any(_leaks(m) for _, m in debug_handler.records),
            )
        )

    return report("DEBUG-TRACE", "debug-trace", checks)
