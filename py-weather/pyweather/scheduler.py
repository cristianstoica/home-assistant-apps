# pyright: strict
"""The adaptive per-station polling scheduler and its interruptible main loop.

`poll_station` performs **one** full poll for a station through the injected
seams (so the ``--check`` oracle drives it deterministically) and mutates that
station's in-memory state — returning the seconds until its next poll. `run_loop`
wraps the per-station polls in a single stop-aware timer loop. Every dependency
is an explicit constructor argument (no module globals).

Scheduling model — the **four-rest table** (each poll returns exactly one):

* ``ONLINE`` → the **learned cadence**. The obstime sensor is present and
  parseable; the poll interval is recomputed from the station's rolling
  ``obsTimeUtc`` window (`cadence.jittered_interval`), so the poller tracks the
  uploader's real cadence with ±15% jitter applied through the injected
  `JitterSource`.
* ``OFFLINE`` → ``cadence.OFFLINE_REPROBE`` (86400). A WU 204 / dead station is
  re-probed once a day; the cadence window is frozen (no event appended).
* ``TERMINAL`` → ``max_backoff_seconds`` (86400). A non-retryable config/token
  fault, raised by the API client, held on the slow cadence.
* ``TransientError`` **and** an interrupted settle wait →
  ``min_interval_seconds`` (300). A transient blip (timeout, 5xx, transient
  429) retries soon at the floor — it is not a dead station and must not inherit
  the daily ``OFFLINE_REPROBE`` cadence. A stop signal that interrupts the
  settle wait raises `TransientError` from `_read_health`, so it funnels into
  the same rest.

**Cadence learning.** On every ONLINE poll, if the read obstime differs from the
last seen one, it is appended to the station's window (truncated to the last
``cadence.N``) and the learned interval is re-derived. The window is seeded from
the persisted ``/data`` boot state, so a restart resumes the learned cadence
instead of cold-starting; the last persisted obstime seeds the dedupe comparand
so the first post-restart poll does not re-append it. After every non-raising
poll the in-memory windows are written back through the debounced best-effort
``save`` seam (a ``/data`` write failure is swallowed, never crashing the loop).

**Advisory stale log.** When the last observed obstime is older than ``3×`` the
unjittered learned interval, an advisory line is logged (the predicate is fed
the unjittered ``base`` so the boundary is stable poll-to-poll). It does not
change the scheduled rest.

**Interruptible everywhere.** The settle wait runs through the single stop-aware
sleeper, so a SIGTERM mid-wait returns promptly and starts no further read/poll.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import NamedTuple, Protocol

from . import cadence
from .config import Config
from .errors import TerminalError, TransientError
from .haapi import HaApiClient
from .health import discover, evaluate
from .models import (
    Clock,
    HealthStatus,
    JitterSource,
    Sleeper,
    Station,
    StationCadence,
    WallClock,
)

_log = logging.getLogger("pyweather")


class SchedulerRunner(Protocol):
    """The run-only seam `StartupDeps.make_scheduler` produces.

    The single method `run_startup` calls on a built scheduler. The real
    `Scheduler` satisfies it structurally (no explicit inheritance), as does the
    `--check` oracle's `_RecordingScheduler` — so the recording factory is
    assignable to the `make_scheduler` callable under pyright-strict with no cast
    or `# type: ignore`, exactly as `SupervisorOptions` does for the persistence
    seam. Typing the seam against this Protocol (not the concrete `Scheduler`) is
    what keeps the `--check` scheduler stand-in type-clean.
    """

    def run_loop(self) -> None: ...


class _HealthRead(NamedTuple):
    """The completed-read result of `_read_health`: the binary health signal and
    the raw ``obsTimeUtc`` string seen this poll (``None`` when offline /
    no parseable observation). Reached only on a settled, completed read — a
    stop during settle raises `TransientError` instead of returning this."""

    status: HealthStatus
    obstime: str | None


class StationState:
    """Mutable per-station scheduling state (in-memory, mirrored to ``/data``).

    `cadence` is the live rolling ``obsTimeUtc`` window (seeded from the boot
    state map); `last_obstime` is the last seen obstime, the dedupe comparand so
    an unchanged observation is not re-appended. Both are seeded from the loaded
    window's tail so the first poll after a restart compares against the
    persisted observation rather than re-appending it.
    """

    def __init__(self, station: Station, state: dict[str, StationCadence]) -> None:
        self.station = station
        seeded = state.get(station.key, StationCadence(events=()))
        self.cadence = seeded
        self.last_obstime = seeded.events[-1] if seeded.events else None


class Scheduler:
    """The adaptive polling driver across all configured stations."""

    def __init__(
        self,
        config: Config,
        *,
        api: HaApiClient,
        clock: Clock,
        wall_clock: WallClock,
        sleeper: Sleeper,
        jitter: JitterSource,
        state: dict[str, StationCadence],
        save: Callable[[dict[str, StationCadence]], None],
    ) -> None:
        self._config = config
        self._api = api
        self._clock = clock
        self._wall_clock = wall_clock
        self._sleeper = sleeper
        self._jitter = jitter
        self._save = save
        self._states = {
            station.key: StationState(station, state) for station in config.stations
        }

    def state_for(self, key: str) -> StationState:
        """Return the mutable `StationState` for `key` (oracle introspection)."""
        return self._states[key]

    def _collect_state(self) -> dict[str, StationCadence]:
        """Snapshot the live per-station cadence windows for the debounced save."""
        return {key: ss.cadence for key, ss in self._states.items()}

    def _read_health(self, station: Station) -> _HealthRead:
        """Settle once, GET /states, evaluate; return ``(status, obstime)``.

        Raises `TransientError` if the stop-aware settle wait is interrupted
        (so the interrupt funnels into `poll_station`'s ``except TransientError``
        rest at ``min_interval_seconds``) — never returns on that path. On a
        completed read the obstime is the raw ``obsTimeUtc`` string when ONLINE
        (the cadence-learning event), else ``None``.
        """
        if self._sleeper(float(self._config.settle_seconds)):
            raise TransientError("stop signalled during settle")

        states = self._api.get_states()
        result = evaluate(station, states)
        obstime: str | None = None
        if result.status is HealthStatus.ONLINE:
            representative = discover(states, station.key).get("obstimeutc")
            obstime = representative.state if representative is not None else None
        _log.debug(
            "%s: poll outcome %s: %s", station.key, result.status.value, result.detail
        )
        return _HealthRead(result.status, obstime)

    def poll_station(self, key: str) -> int:
        """Run one full poll for `key`, mutate its state, return seconds-to-next-poll.

        Sequence: POST ``update_entity``, settle + read + evaluate, then schedule
        per the four-rest table:
        ONLINE→learned cadence, OFFLINE→``OFFLINE_REPROBE``,
        TERMINAL→``max_backoff_seconds``, TransientError/interrupted-settle→
        ``min_interval_seconds``. After mutating state the in-memory windows are
        written back once through the debounced best-effort save seam.
        """
        state = self._states[key]
        station = state.station
        try:
            self._api.update_entity(station.update_entity)
            read = self._read_health(station)
        except TerminalError as exc:
            _log.error(
                "%s: terminal fault; holding at max_backoff (%ds): %s",
                station.key,
                self._config.max_backoff_seconds,
                exc,
            )
            return self._config.max_backoff_seconds
        except TransientError as exc:
            _log.warning(
                "%s: transient poll failure; retrying at floor (%ds): %s",
                station.key,
                self._config.min_interval_seconds,
                exc,
            )
            return self._config.min_interval_seconds

        if read.status is HealthStatus.ONLINE:
            if read.obstime is not None and read.obstime != state.last_obstime:
                events = (*state.cadence.events, read.obstime)[-cadence.N :]
                state.cadence = StationCadence(events=events)
                state.last_obstime = read.obstime
            base = cadence.base_interval(
                state.cadence.events, self._config.min_interval_seconds
            )
            interval = cadence.jittered_interval(
                state.cadence.events, self._config.min_interval_seconds, self._jitter
            )
            if cadence.is_stale(state.cadence.events, self._wall_clock.now(), base):
                _log.warning(
                    "%s: last observation is stale (> 3x the %ds learned interval); "
                    "the station may have stopped uploading",
                    station.key,
                    base,
                )
            _log.info("%s: online; next poll in %ds", station.key, interval)
            self._save(self._collect_state())
            return interval

        # OFFLINE (WU 204 / dead station): freeze the window, re-probe daily.
        _log.info(
            "%s: offline; re-probing in %ds", station.key, cadence.OFFLINE_REPROBE
        )
        self._save(self._collect_state())
        return cadence.OFFLINE_REPROBE

    def run_loop(self) -> None:
        """Run the adaptive loop across all stations until stop is signalled.

        Cold start: every station begins at the ``min_interval_seconds`` floor
        (no learned history yet). First polls are staggered by
        ``startup_stagger_seconds`` to avoid an 8-request burst. Each station's
        next-poll time is tracked in monotonic seconds; the loop sleeps to the
        nearest due station through the single stop-aware sleeper.
        """
        now = self._clock()
        next_poll: dict[str, float] = {}
        for index, station in enumerate(self._config.stations):
            next_poll[station.key] = now + index * self._config.startup_stagger_seconds
        _log.info(
            "py-weather starting: %d station(s); cold-start cadence %ds",
            len(self._config.stations),
            self._config.min_interval_seconds,
        )
        for station in self._config.stations:
            _log.info("registered station %s -> %s", station.key, station.update_entity)

        while True:
            now = self._clock()
            due_key = min(next_poll, key=lambda k: next_poll[k])
            wait = max(0.0, next_poll[due_key] - now)
            if self._sleeper(wait):
                _log.info("stop signalled; exiting scheduler loop")
                return
            delay = self.poll_station(due_key)
            next_poll[due_key] = self._clock() + float(delay)
