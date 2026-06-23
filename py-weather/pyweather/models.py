# pyright: strict
"""Data structures and the seams (Protocols) the scheduler runs against.

`NamedTuple`s (not `@dataclass`) match the repo idiom (py-ddns / py-syslog
`models.py`). Every runtime dependency the scheduler touches — the HTTP
transport, the monotonic clock, the wall-clock instant source, and the
interruptible sleeper — is a `Protocol` so the ``--check`` oracle drives the
whole poll/backoff cycle synchronously against recording fakes (no network, no
real sockets/threads).

The poll outcome is a **four-way** classification, never collapsed to a bare
healthy/unhealthy bool: a healthy poll is split into *positively-confirmed*
(earns the fast-cadence reward) versus *inconclusive-fallback accept* (held at
the prior/slow cadence, never rewarded), and an unhealthy poll is split into
*transient* (exponential backoff) versus *terminal* (slow ``max_backoff_seconds``
hold, no doubling). Collapsing any of these would let a masked outage be
rewarded with the fast cadence, or spin a doomed terminal retry tight.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import NamedTuple, Protocol


class Station(NamedTuple):
    """One validated Weather.com PWS station to poll.

    `key` is the lowercase-alphanumeric station id (the contract-bearing
    entity-id suffix interpolated into both the ``update_entity`` matcher and the
    runtime discovery glob ``sensor.wu_*_<key>``). `update_entity` is the pinned,
    fully-slugified representative entity-id (``sensor.wu_temp_<key>``) the
    refresh POST targets. `expected_sensors` is the **soft** full-count signal
    (logged when short, never a hard health gate); the hard floor is the
    required-core subset (``temp``/``humidity``/``pressure``).
    """

    key: str
    update_entity: str
    expected_sensors: int


class Config(NamedTuple):
    """Validated, fully-resolved runtime configuration.

    Every duration field is range-validated in `config` against an explicit
    allowlist bound (no open-ended field). An **empty** `stations` is legal
    **only** as the startup auto-populate trigger (resolved at runtime from
    discovery); a **populated** `stations` still has unique, regex-literal keys,
    and the `Scheduler` is only ever constructed with a non-empty `stations`
    tuple. `reread_interval_seconds` is an internal cadence (not a user option)
    reused from `settle_seconds` so the bounded freshness re-reads are spaced
    through the single sleeper with no additional config surface.
    """

    healthy_interval_min: int
    healthy_interval_max: int
    initial_backoff_seconds: int
    max_backoff_seconds: int
    settle_seconds: int
    startup_stagger_seconds: int
    request_timeout_seconds: int
    log_level: str
    stations: tuple[Station, ...]


class HealthStatus(str, Enum):
    """The four-way per-poll classification (never a bare healthy/unhealthy bool).

    `CONFIRMED` — healthy and positively confirmed (primary ``last_reported``
    advanced, or a fallback ``last_updated``/``last_changed`` advanced past
    ``t0``): earns the reset-to-floor + fast-cadence reward.

    `INCONCLUSIVE` — healthy *only* by the degrade-safely accept (POST succeeded,
    representative present, required-core usable, but the fallback timestamp did
    not advance and no primary ``last_reported`` was available): accepted, not
    backed off, but **not** rewarded with the fast cadence.

    `UNHEALTHY` — a transient failure: a missing/unusable required-core sensor, a
    failed freshness check on the primary path, or a transient API/parse failure.
    Enters exponential backoff.

    `TERMINAL` — a non-retryable config/token fault. Held on the slow
    ``max_backoff_seconds`` cadence, never the doubling sequence.
    """

    CONFIRMED = "confirmed"
    INCONCLUSIVE = "inconclusive"
    UNHEALTHY = "unhealthy"
    TERMINAL = "terminal"


class HealthResult(NamedTuple):
    """The outcome of evaluating one station poll.

    `status` is the four-way classification; `detail` is a secret-free human
    string for the log line; `discovered` is the count of station sensors
    discovered in ``/states`` (logged against `expected_sensors` as the soft
    signal).
    """

    status: HealthStatus
    detail: str
    discovered: int


class EntityState(NamedTuple):
    """One Home Assistant entity's relevant ``/states`` projection.

    `state` is the string state value; the three timestamp fields are the raw
    ISO-8601 strings as carried in the payload, or ``None`` when the key is
    absent or present-but-``null``/empty (the freshness check decides the path
    per-read from these, never assuming the new-Core `last_reported` key exists).
    """

    entity_id: str
    state: str
    last_reported: str | None
    last_updated: str | None
    last_changed: str | None


class Clock(Protocol):
    """Monotonic seconds source, injected so scheduling is deterministic in tests."""

    def __call__(self) -> float: ...


class WallClock(Protocol):
    """Timezone-aware UTC wall-clock instant source for the freshness ``t0``.

    Returns a tz-aware UTC ``datetime`` (``datetime.now(timezone.utc)`` in
    production) — explicitly NOT a monotonic value, which cannot be compared to
    Home Assistant state timestamps. The ``--check`` oracle injects a fixed
    instant so freshness comparisons are deterministic.
    """

    def now(self) -> datetime: ...


class Sleeper(Protocol):
    """Interruptible sleep seam.

    Sleeps `seconds`, returning early if the stop signal fires; returns ``True``
    iff stop was signalled (so the caller aborts before its next attempt or
    re-read). The real impl is a ``threading.Event.wait``; the ``--check`` oracle
    injects a fully synchronous fake — **no real ``Timer`` / thread**.
    """

    def __call__(self, seconds: float) -> bool: ...
