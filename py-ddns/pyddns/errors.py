# pyright: strict
"""Domain error taxonomy shared by the providers and the updater.

The updater reconciles by classifying every provider failure into exactly one of
two outcomes:

* `TerminalError` — a condition that will not self-heal by retrying (a bad SP
  secret, a 403, a bad/disabled callback URL). The updater logs it loudly and
  **holds last-good**, never spinning a doomed retry.
* `TransientError` — a 429 / 5xx / network / timeout. The updater runs the
  bounded, interruptible backoff and, on exhaustion, waits for the next cycle.

Both messages are already secret-free (the providers sanitize before raising).
"""

from __future__ import annotations


class TerminalError(Exception):
    """A non-retryable provider failure; the updater holds last-good and logs loudly."""


class TransientError(Exception):
    """A retryable provider failure; the updater runs bounded interruptible backoff.

    `retry_after` carries a server-suggested delay (seconds, parsed from a
    ``Retry-After`` header) when present, else ``None``; the updater caps it and
    uses it to drive the bounded delay sequence.
    """

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after
