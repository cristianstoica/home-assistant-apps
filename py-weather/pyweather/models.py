# pyright: strict
"""Data structures and the seams (Protocols) the scheduler runs against.

`NamedTuple`s (not `@dataclass`) match the repo idiom (py-ddns / py-syslog
`models.py`). Every runtime dependency the scheduler touches — the HTTP
transport, the monotonic clock, the wall-clock instant source, and the
interruptible sleeper — is a `Protocol` so the ``--check`` oracle drives the
whole poll/backoff cycle synchronously against recording fakes (no network, no
real sockets/threads).

Health is **binary** data-presence: a poll is ``ONLINE`` when the station's
``obstimeutc`` sensor is present and parses as a timestamp, ``OFFLINE`` when it
is absent / ``unavailable`` / unparseable (plus the API-client ``TERMINAL``
fault, raised by the client, never by ``evaluate``). The poll interval is not
fixed: it is auto-learned per station from the observed gaps between successive
obstimes, clamped to ``[min_interval_seconds, 1800]`` and jittered ±15%. The
scheduler maps each outcome to one of four rests — the learned (jittered)
cadence when online, ``OFFLINE_REPROBE`` (86400) when offline, the flat
``min_interval_seconds`` floor on a transient failure, and ``max_backoff_seconds``
on a terminal fault.
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
    refresh POST targets. `expected_sensors` is the **soft** full-count signal:
    a shortfall is logged as a non-fatal advisory, never a health gate. Health
    keys off ``obstimeutc`` alone, with no required-core sensor subset.
    """

    key: str
    update_entity: str
    expected_sensors: int


class Config(NamedTuple):
    """Validated, fully-resolved runtime configuration.

    Every duration field is range-validated in `config` against an explicit
    allowlist bound (no open-ended field). `min_interval_seconds` is the floor
    for the learned poll interval (bounded ``60-1800``: the upper bound is the
    fixed healthy-slow-uploader ceiling, never above it). An **empty** `stations`
    is legal
    **only** as the startup auto-populate trigger (resolved at runtime from
    discovery); a **populated** `stations` still has unique, regex-literal keys,
    and the `Scheduler` is only ever constructed with a non-empty `stations`
    tuple. `settle_seconds` is reused for two waits through the single sleeper —
    the per-poll settle before each `/states` read and the spacing between the
    startup discovery attempts — with no additional config surface.
    """

    max_backoff_seconds: int
    min_interval_seconds: int
    settle_seconds: int
    startup_stagger_seconds: int
    request_timeout_seconds: int
    log_level: str
    stations: tuple[Station, ...]


class HealthStatus(str, Enum):
    """The per-poll health signal: data-present (online) vs data-absent
    (offline), plus the API-client terminal fault.

    `ONLINE` — `sensor.wu_obstimeutc_<key>` is present and a parseable
    timestamp: WU is serving an observation. Polled at the learned cadence.

    `OFFLINE` — the obstime sensor is absent / `unavailable` / unparseable: WU
    served a 204 (dead station). Re-probed once a day (`OFFLINE_REPROBE`).

    `TERMINAL` — a non-retryable config/token fault, raised by the API client,
    never by `evaluate`. Held on the slow `max_backoff_seconds` cadence.
    """

    ONLINE = "online"
    OFFLINE = "offline"
    TERMINAL = "terminal"


class HealthResult(NamedTuple):
    """The outcome of evaluating one station poll.

    `status` is the binary `HealthStatus`; `detail` is a secret-free human
    string for the log line; `discovered` is the count of station sensors
    discovered in ``/states`` (logged against `expected_sensors` as the soft
    signal).
    """

    status: HealthStatus
    detail: str
    discovered: int


class EntityState(NamedTuple):
    """One Home Assistant entity's relevant ``/states`` projection.

    `entity_id` is the full HA entity id; `state` is its string state value. The
    health check reads only these two: an observation's presence and parseability
    is judged from the obstime sensor's `state`, with no timestamp metadata.
    """

    entity_id: str
    state: str


class StationCadence(NamedTuple):
    """The persisted per-station cadence window: the last N raw obsTimeUtc
    strings, newest last. The poll interval is recomputed from these on every
    load (the design stores raw events, not a derived period, so a future
    estimator change re-derives from history)."""

    events: tuple[str, ...]


class JitterSource(Protocol):
    """Injected jitter seam: maps a base interval to a value within ±15% of it.

    Production is a `random.Random.uniform(base*0.85, base*1.15)` wrapper; the
    `--check` oracle injects a fixed-factor fake so the band assertion is
    deterministic (no live RNG)."""

    def __call__(self, base: float) -> float: ...


class Clock(Protocol):
    """Monotonic seconds source, injected so scheduling is deterministic in tests."""

    def __call__(self) -> float: ...


class WallClock(Protocol):
    """Timezone-aware UTC wall-clock instant source for the stale-advisory log.

    Returns a tz-aware UTC ``datetime`` (``datetime.now(timezone.utc)`` in
    production) — explicitly NOT a monotonic value, which cannot be compared to
    Home Assistant state timestamps. The scheduler compares this instant against
    the last observed obstime to decide whether to emit the advisory "stale"
    WARNING (it does NOT gate health or scheduling). The ``--check`` oracle
    injects a fixed instant so that comparison is deterministic.
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
