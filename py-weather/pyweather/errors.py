# pyright: strict
"""Domain error taxonomy shared by the HA API client and the scheduler.

The scheduler classifies every API failure into exactly one of two outcomes
(the py-ddns taxonomy):

* `TerminalError` — a config/token fault that will not self-heal by retrying (a
  revoked/insufficient ``SUPERVISOR_TOKEN``, a wrong Core-API path, a
  misconfigured ``update_entity`` target). The scheduler logs it at ``error``
  and holds the station on the **slow** ``max_backoff_seconds`` cadence, so a
  doomed retry cannot spin tight while still letting the station self-heal on
  the next slow poll if corrected out-of-band.
* `TransientError` — a retryable failure (transport/connection, timeout, ``5xx``,
  ``429``, or a malformed/non-JSON ``/states`` body). The scheduler logs it at
  ``warning`` and rests the station at the flat ``min_interval_seconds`` floor
  (no backoff growth) so it re-probes promptly.

Both messages are already secret-free (the API client redacts before raising).
"""

from __future__ import annotations


class TerminalError(Exception):
    """A non-retryable API/config fault.

    The scheduler logs it at ``error`` and holds the station on the slow
    ``max_backoff_seconds`` cadence.
    """


class TransientError(Exception):
    """A retryable API/poll failure; the station rests at the flat
    ``min_interval_seconds`` floor (no backoff growth)."""
