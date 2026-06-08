# pyright: strict
"""The archetype-aware reconcile loop and the bounded interruptible backoff.

`run_once` performs **one** reconcile cycle through the injected seams (so the
``--check`` oracle drives it deterministically); `run_loop` wraps it in the
interval timer until `stop` is signalled. Every dependency is an explicit
parameter (no module globals).

Key invariants (from the plan):

* **First cycle on start is authoritative.** The provider's *real* current value
  is read (API: ``read_current()``; callback: DNS-resolve `name`) and acted on if
  missing/stale — local state never suppresses a startup self-heal, even with
  ``drift_reconcile_seconds == 0``.
* **API archetype** skips when no valid IP, writes only on change vs
  last-good/``read_current()``, and persists the **detected IP** on a 2xx PUT.
* **Callback archetype** fires only when it has reason to (first cycle, drift
  cycle, detected-IP change, or a previously-unconfirmed fire) but **suppresses a
  fire while `name` already resolves to the persisted last-known value**. A fire
  is confirmed by a **post-fire resolve** whose three-way outcome gates
  persistence:
  - resolved == a concrete global value → confirmed: persist that resolved value;
  - resolved == stale/no-record → unconfirmed: do not persist, refire next cycle,
    log the distinct *unconfirmed* diagnostic;
  - resolve failed/transient → inconclusive: retry within budget, then **hold**
    last-known (not cleared), log the distinct *inconclusive* diagnostic.

Backoff for transient failures runs through the injected synchronous
`clock`/`sleeper` **only** — never a real ``Timer`` / thread.
"""

from __future__ import annotations

import logging
import secrets
from collections.abc import Callable
from ipaddress import IPv4Address

from .errors import TerminalError, TransientError
from .models import (
    ApplyAction,
    Clock,
    Config,
    DnsProvider,
    IpSource,
    Provider,
    Resolver,
    ResolveStatus,
    Sleeper,
)
from .redact import sanitize
from .state import State

_log = logging.getLogger("pyddns")

# Bounded transient-retry policy (per the plan): 3 attempts, 2->4->8 capped 30,
# +/-20% jitter; Retry-After honored but capped at 60.
MAX_ATTEMPTS = 3
BASE_DELAY_S = 2.0
MAX_DELAY_S = 30.0
RETRY_AFTER_CAP_S = 60.0
JITTER_FRACTION = 0.20
# Post-fire confirmation resolve gets one retry inside the per-cycle budget.
_CONFIRM_RETRIES = 2


def _jittered(delay: float) -> float:
    """Apply +/-20% jitter to `delay` using a non-cryptographic spread.

    `secrets.randbelow` gives a deterministic-when-seeded-free integer source; the
    jitter only needs to de-correlate retries across instances, not be secure.
    """
    span = delay * JITTER_FRACTION
    # randbelow(2001) / 1000 - 1 -> a value in [-1.0, +1.0].
    factor = secrets.randbelow(2001) / 1000.0 - 1.0
    return max(0.0, delay + span * factor)


def backoff_delays(retry_after: float | None) -> list[float]:
    """The base (pre-jitter) delay sequence for the transient-retry budget.

    ``2 -> 4 -> 8`` capped at 30; a present ``Retry-After`` replaces each delay
    (capped at 60). Length is ``MAX_ATTEMPTS - 1`` (delays sit *between*
    attempts). Exposed so the oracle can pin the bounded sequence behaviorally.
    """
    delays: list[float] = []
    for i in range(MAX_ATTEMPTS - 1):
        base = min(BASE_DELAY_S * (2**i), MAX_DELAY_S)
        if retry_after is not None:
            base = min(retry_after, RETRY_AFTER_CAP_S)
        delays.append(base)
    return delays


class RetryRunner:
    """Runs a transient-failing operation under the bounded interruptible backoff.

    The clock/sleeper are injected so the ``--check`` oracle drives the whole
    backoff synchronously — no real ``Timer``/thread. `attempts` and `slept` are
    recorded so the oracle can assert the exact attempt count and delay sequence.
    """

    def __init__(self, clock: Clock, sleeper: Sleeper) -> None:
        self._clock = clock
        self._sleeper = sleeper
        self.attempts = 0
        self.slept: list[float] = []

    def run(self, op: Callable[[], None]) -> bool:
        """Run `op` up to `MAX_ATTEMPTS` times across transient failures.

        Returns ``True`` on success. On a `TransientError`, sleeps the bounded,
        jittered delay through the injected sleeper; if `stop` fires mid-backoff
        the runner returns ``False`` **before the next attempt** (no further
        attempt or sleep). A `TerminalError` propagates immediately (no retry).
        On exhaustion returns ``False`` (give up this cycle). A `TransientError`
        may carry a ``retry_after`` attribute (the providers thread the HTTP
        ``Retry-After`` through), which then drives the bounded delay.
        """
        retry_after: float | None = None
        for attempt in range(MAX_ATTEMPTS):
            self.attempts += 1
            try:
                op()
                return True
            except TransientError as exc:
                if exc.retry_after is not None:
                    retry_after = exc.retry_after
                if attempt == MAX_ATTEMPTS - 1:
                    _log.warning("transient failure, retries exhausted: %s", exc)
                    return False
                delay = _jittered(backoff_delays(retry_after)[attempt])
                _log.warning("transient failure (attempt %d): %s", attempt + 1, exc)
                self.slept.append(delay)
                if self._sleeper(delay):
                    return (
                        False  # stop signalled mid-backoff: abort before next attempt
                    )
        return False  # pragma: no cover - loop always returns inside


class Updater:
    """The archetype-aware reconcile driver."""

    def __init__(
        self,
        config: Config,
        *,
        ip_source: IpSource,
        provider: DnsProvider,
        resolver: Resolver,
        state: State,
        clock: Clock,
        sleeper: Sleeper,
    ) -> None:
        self._config = config
        self._ip_source = ip_source
        self._provider = provider
        self._resolver = resolver
        self._state = state
        self._clock = clock
        self._sleeper = sleeper
        self._first_cycle = True
        # Monotonic time of the last authoritative drift re-assert.
        self._last_drift = clock()

    def mark_started(self) -> None:
        """Declare the startup self-heal already done (no-op if past the first cycle).

        Public lifecycle hook: it can only move the updater *out* of its
        authoritative first cycle, never back into it, so it cannot be used to skip
        the startup self-heal. Used by the ``--check`` oracle to assert the
        steady-state (non-first-cycle) reconcile branches directly.
        """
        self._first_cycle = False

    def run_loop(self) -> None:
        """Run cycles forever, sleeping `interval_seconds` between, until stop.

        The interval sleep runs through the injected interruptible `sleeper`, so
        SIGTERM/SIGINT aborts promptly. `run_once` never raises a transient/
        terminal error to here (each is caught and the loop continues).
        """
        while True:
            self.run_once()
            if self._sleeper(float(self._config.interval_seconds)):
                _log.info("stop signalled; exiting reconcile loop")
                return

    def run_once(self) -> None:
        """Run one reconcile cycle, dispatching by archetype. Never raises."""
        try:
            if self._config.provider is Provider.AZURE:
                self._cycle_api()
            else:
                self._cycle_callback()
        except TerminalError as exc:
            _log.error("terminal failure this cycle; holding last-good: %s", exc)
        except TransientError as exc:
            _log.warning(
                "transient failure this cycle; will retry next interval: %s", exc
            )
        except Exception as exc:  # noqa: BLE001 - contract backstop, not the fix
            # run_once must never escape to s6: an escape exits the process and
            # triggers the s6 restart loop. The "Never raises" contract is
            # load-bearing. Part A's config-load validation is the actual fix;
            # this clause only converts a slipped-through bug into a held cycle.
            # The exc string can echo the secret url_endpoint, so scrub it.
            safe = sanitize(str(exc), (self._config.url_endpoint,))
            _log.error("unexpected error this cycle; holding last-good: %s", safe)
        finally:
            self._first_cycle = False

    def _drift_due(self) -> bool:
        """True when the authoritative drift re-assert cadence has elapsed (``0`` = off)."""
        if self._config.drift_reconcile_seconds <= 0:
            return False
        return (
            self._clock() - self._last_drift
        ) >= self._config.drift_reconcile_seconds

    # --- API archetype (azure) ---------------------------------------------

    def _cycle_api(self) -> None:
        """API reconcile: read authoritative current, skip-if-no-IP, write-on-change.

        On the first cycle (or a drift cycle) the provider's real current value is
        read so a missing/stale record is healed regardless of local state. The
        detected IP is persisted only on a confirmed 2xx PUT.
        """
        detected = self._ip_source.detect()
        _log.debug("%s -> ip detection: detected=%s", self._config.name, detected)
        if detected is None:
            _log.warning(
                "%s -> no valid egress IP this cycle; holding last-good (no write)",
                self._config.name,
            )
            return

        authoritative = self._first_cycle or self._drift_due()
        if authoritative:
            current = self._read_current_with_retry()
            self._last_drift = self._clock()
            _log.debug(
                "%s -> update decision: authoritative read current=%s detected=%s",
                self._config.name,
                current,
                detected,
            )
            if current == detected:
                _log.info("%s -> %s (matches ✓)", self._config.name, detected)
                self._state.write(detected)
                return
        else:
            last_known = self._state.read()
            _log.debug(
                "%s -> update decision: steady last-known=%s detected=%s",
                self._config.name,
                last_known,
                detected,
            )
            if last_known == detected:
                _log.info("%s -> %s (unchanged; no write)", self._config.name, detected)
                return

        self._apply_api_with_retry(detected)

    def _read_current_with_retry(self) -> IPv4Address | None:
        runner = RetryRunner(self._clock, self._sleeper)
        box: list[IPv4Address | None] = [None]

        def _op() -> None:
            box[0] = self._provider.read_current()

        if not runner.run(_op):  # transient exhaustion / stop
            raise TransientError("read_current did not complete this cycle")
        return box[0]

    def _apply_api_with_retry(self, detected: IPv4Address) -> None:
        runner = RetryRunner(self._clock, self._sleeper)
        result_box: list[ApplyAction | None] = [None]

        def _op() -> None:
            result = self._provider.apply(detected)
            result_box[0] = result.action

        if not runner.run(_op):
            raise TransientError("apply did not complete this cycle")
        _log.debug(
            "%s -> confirmation: apply action=%s detected=%s",
            self._config.name,
            result_box[0].value if result_box[0] is not None else None,
            detected,
        )
        if result_box[0] is ApplyAction.WROTE_KNOWN_IP:
            self._state.write(detected)
            _log.info("%s -> %s (wrote A record)", self._config.name, detected)

    # --- callback archetype (url) ------------------------------------------

    def _cycle_callback(self) -> None:
        """Callback reconcile: suppress-while-steady, fire-and-confirm otherwise.

        A fire is suppressed while `name` already resolves to the persisted
        last-known value (the steady state for the default server-detection mode).
        Otherwise it fires, then confirms by a post-fire resolve whose three-way
        outcome gates whether last-known is persisted.
        """
        detected = self._ip_source.detect()  # optional change-trigger for url
        # The callback archetype defers authoritative IP detection to the server;
        # `detected` is only the optional local change-trigger.
        _log.debug(
            "%s -> ip detection: detection deferred to server; local trigger=%s",
            self._config.name,
            detected,
        )
        last_known = self._state.read()
        drift = self._drift_due()
        if drift:
            self._last_drift = self._clock()

        # Suppress a fire only in steady state: not a drift cycle, not the first
        # cycle, we have a last-known, the detected IP (if any) has not changed,
        # and `name` still resolves to that last-known value.
        if not self._first_cycle and not drift and last_known is not None:
            if detected is None or detected == last_known:
                outcome = self._resolver.resolve(self._config.name)
                if (
                    outcome.status is ResolveStatus.RESOLVED
                    and outcome.value == last_known
                ):
                    _log.debug(
                        "%s -> update decision: steady (resolve==last-known); "
                        "suppressing fire",
                        self._config.name,
                    )
                    _log.info(
                        "%s -> %s (steady; suppressing fire)",
                        self._config.name,
                        last_known,
                    )
                    return

        _log.debug(
            "%s -> update decision: firing callback "
            "(first_cycle=%s drift=%s last_known=%s)",
            self._config.name,
            self._first_cycle,
            drift,
            last_known,
        )
        self._fire_and_confirm(detected)

    def _fire_and_confirm(self, detected: IPv4Address | None) -> None:
        """Fire the callback then gate persistence on the post-fire resolve."""
        runner = RetryRunner(self._clock, self._sleeper)
        action_box: list[ApplyAction | None] = [None]

        def _op() -> None:
            action_box[0] = self._provider.apply(detected).action

        if not runner.run(_op):
            raise TransientError("callback fire did not complete this cycle")

        # Post-fire confirmation: resolve `name`, retry within budget on a
        # transient resolve failure, then gate on the three-way outcome.
        outcome = self._resolver.resolve(self._config.name)
        retries = 0
        while outcome.status is ResolveStatus.FAILED and retries < _CONFIRM_RETRIES:
            retries += 1
            outcome = self._resolver.resolve(self._config.name)

        _log.debug(
            "%s -> confirmation: post-fire resolve status=%s value=%s retries=%d",
            self._config.name,
            outcome.status.value,
            outcome.value,
            retries,
        )
        if outcome.status is ResolveStatus.RESOLVED and outcome.value is not None:
            self._state.write(outcome.value)
            if detected is not None:
                suffix = "matches ✓" if outcome.value == detected else "server-detected"
            else:
                suffix = "server-detected"
            _log.info("%s -> %s (%s)", self._config.name, outcome.value, suffix)
        elif outcome.status is ResolveStatus.FAILED:
            _log.warning(
                "%s -> post-fire confirmation inconclusive (transient resolve "
                "failure); holding last-known",
                self._config.name,
            )
        else:  # NO_RECORD / stale: fired but DNS not yet updated
            _log.warning(
                "%s -> (unconfirmed — fired, DNS not yet updated)",
                self._config.name,
            )
